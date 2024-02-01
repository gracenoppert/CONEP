local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'append_states_zipcode_dates_merge_census_vars_`version'.log", replace

/*
This code appends all of the State zip code level data into a single file
*/

local state_names "Arizona Florida Illinois Indiana Maine Maryland Minnesota NorthCarolina Nevada NewYork Ohio Oklahoma Oregon Pennsylvania Vermont Virginia"
local state_fips_codes "04 12 17 18 23 24 27 37 32 36 39 40 41 42 44 50 51"
* states with dates available
local state_date_names "Maryland Minnesota Nevada NewYork Vermont Virginia"
local data_dir "D:/covid/Noppert/data/Covid-19Data/ZipcodeLevel/"
*local infile_stubs ""
local outfile "`data_dir'covid_all_states_zipcode_dates_`version'"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zip_county_xwalk "D:/covid/Noppert/data/best_zip_county_062020.dta"
local censusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"
local statepop17file "D:/Census/ACS/2017/2013-2017/data/Tracts_Block_Groups_Only/state_pop_nanda_ses_tract_2008-2017_04.dta"
local statepop18file "D:/Census/ACS/2018/data/Tracts_Block_Groups_Only/state_pop18_sfe_all_us_stack_tract_ses_sub_v2.dta"
local statepop20file "D:/Census/ACS/2020/data/Tracts_Block_Groups_Only/state_fips_pop_acs_2016_2020_tract_ses_subset.dta"
local politicalfile "O:/NaNDA/Data/voting/voting_county_2004-2018/datasets/nanda_voting_county_2018_01P"
local zip10rucafile "D:/Census/ruca/data/zip_2010_ruca_4cat"
local mergefile "`outfile'_merge_ses"

cd "`data_dir'"

* loop through each state
local num_files : word count `state_names'
forvalues i=1/`num_files' {

	display "`i'"
	local state_name : word `i' of `state_names'
	local state_fips_code : word `i' of `state_fips_codes'
	
	* filenames change if has dates
	local infile_stub "`state_name'_covid_zipcode_date_subset_`version'"
	capture confirm file "`infile_stub'.dta"
	if _rc==0 {
		display "Found date file `infile_stub' for `state_name', proceed"
	}
	else {
		display "Did not find date file `infile_stub' for `state_name', try no date file"
		local infile_stub "`state_name'_covid_zipcode_subset_`version'.dta"
	}
	
	* make sure infiles only represent one state
	* this should be done
	/*
	use "`infile_stub'", clear
	keep if state_fips == "`state_fips_code'"
	save "`infile_stub'", replace
	*/
	* append into one file
	if `i' == 1 {
		* start an all states file
		use "`infile_stub'", clear
		save "`outfile'", replace
	}
	else {
		* append to all file
		use "`outfile'", clear
		append using "`infile_stub'", force
		save "`outfile'", replace
	}
}

* merge in ACS SES vars
use "`outfile'", clear

gen zip_data = 1

* flag the states with dates / phase data
gen has_dates = 0
replace has_dates = 1 if inlist(state_fips, "24", "27", "32", "36", "50", "51")

* drop invalid zip code rows
drop if !regexm(zip_code, "^[0-9]+$")
recast str5 zip_code, force

* drop missing zip with no case data
duplicates tag zip_code, gen(tag_dup_zip)
drop if tag_dup_zip==1 & missing(cases)

count
merge 1:m zip_code using "`zip_zcta_xwalk'", keep(match master) gen(_merge_xwalk)
* multiple zip codes can match to a single zcta
count
gen zcta10 = zcta
merge m:1 zcta10 using "`censusfile'", keep(master match) gen(_merge_ses)
count


*gen stcofips = substr(tract_fips10,1,5)
replace cases = zip_cases if missing(cases) 

* replace covid cases per 10k, where missing 
replace cases_per_10k = cases / (totpop13_17/10000) if missing(cases_per_10k)
* set over 1 per person to missing
replace cases_per_10k = . if cases_per_10k > 10000 & !missing(cases_per_10k)

capture gen cases_per_100k = cases_per_10k * 10
replace cases_per_100k = cases_per_10k * 10 if missing(cases_per_100k)
* set over 1 per person to missing
replace cases_per_100k = . if cases_per_100k > 100000 & !missing(cases_per_100k)



local myvars "zip_phase1_cases zip_phase2_cases zip_phase3_cases zip_phase4_cases zip_cases"
foreach myvar of local myvars {
    gen `myvar'_per_10k = `myvar' / (totpop13_17/10000)
	* set over 1 per person to missing
	replace `myvar'_per_10k = . if `myvar'_per_10k > 10000 & !missing(`myvar'_per_10k)
	
	gen `myvar'_per_100k = `myvar'_per_10k * 10
	* set over 1 per person to missing
	replace `myvar'_per_100k = . if `myvar'_per_100k > 100000 & !missing(`myvar'_per_100k)
}


egen tot_state_cases = total(cases), by(state_fips)

egen tot_state_phase1_cases = total(zip_phase1_cases) if has_dates==1, by(state_fips)
egen tot_state_phase2_cases = total(zip_phase2_cases) if has_dates==1, by(state_fips)
egen tot_state_phase3_cases = total(zip_phase3_cases) if has_dates==1, by(state_fips)
egen tot_state_phase4_cases = total(zip_phase4_cases) if has_dates==1, by(state_fips)

* get state population
local statepop17file "D:/Census/ACS/2017/2013-2017/data/Tracts_Block_Groups_Only/state_pop_nanda_ses_tract_2008-2017_04.dta"
merge m:1 state_fips using "`statepop17file'", keep(master match) gen(_merge_state_totpop17)

gen tot_state_cases_per_100k = tot_state_cases / (state_totpop13_17/100000)
*replace state_cases = tot_state_cases if missing(state_cases)

gen tot_state_phase1_cases_per_100k = tot_state_phase1_cases / (state_totpop13_17/100000)
gen tot_state_phase2_cases_per_100k = tot_state_phase2_cases / (state_totpop13_17/100000)
gen tot_state_phase3_cases_per_100k = tot_state_phase3_cases / (state_totpop13_17/100000)
gen tot_state_phase4_cases_per_100k = tot_state_phase4_cases / (state_totpop13_17/100000)

egen state_casesper100k_p75 = pctile(cases_per_100k), p(75) by(state_fips)
gen hi75_casesper100k_state = (cases_per_100k >= state_casesper100k_p75 & !missing(cases_per_100k))

local numcats "4"
local myvars "cases_per_100k zip_phase1_cases_per_100k zip_phase2_cases_per_100k zip_phase3_cases_per_100k zip_phase4_cases_per_100k"
foreach myvar of local myvars {
	*egen `myvar'_p75 = pctile(`myvar'), p(75) by(state_fips)
	*gen hi75_`myvar' = (`myvar' >= `myvar'_p75 & !missing(`myvar'))
	*replace hi75_`myvar' = . if has_dates != 1
	* make State quantiles
	egen `myvar'_`numcats'tile = xtile(`myvar'), n(`numcats') by(state_fips)
	tab `myvar'_`numcats'tile, gen(`myvar'_`numcats'tile)
	gen hi75_`myvar' = `myvar'_`numcats'tile4
	bigtab hi75_`myvar' `myvar'_`numcats'tile `myvar'
}

fsum zip_phase1_cases_per_100k hi75_zip_phase1_cases_per_100k zip_phase2_cases_per_100k hi75_zip_phase2_cases_per_100k zip_phase3_cases_per_100k hi75_zip_phase3_cases_per_100k zip_phase4_cases_per_100k hi75_zip_phase4_cases_per_100k
fsum zip_phase1_cases_per_100k hi75_zip_phase1_cases_per_100k zip_phase2_cases_per_100k hi75_zip_phase2_cases_per_100k zip_phase3_cases_per_100k hi75_zip_phase3_cases_per_100k zip_phase4_cases_per_100k hi75_zip_phase4_cases_per_100k if has_dates==1
fsum zip_phase1_cases_per_100k hi75_zip_phase1_cases_per_100k zip_phase2_cases_per_100k hi75_zip_phase2_cases_per_100k zip_phase3_cases_per_100k hi75_zip_phase3_cases_per_100k zip_phase4_cases_per_100k hi75_zip_phase4_cases_per_100k if has_dates==0

local zip_county_xwalk "D:/covid/Noppert/data/best_zip_county_062020.dta"
merge m:1 zip_code using "`zip_county_xwalk'", keep(master match) gen(_merge_zipcoxwalk)

clonevar stcofips = county

* merge in political partisanship
local politicalfile "O:/NaNDA/Data/voting/voting_county_2004-2018/datasets/nanda_voting_county_2018_01P"
merge m:1 stcofips using "`politicalfile'", keep(master match) gen(_merge_political)
fsum partisan_index_rep
*codebook

* merge in ruca
merge 1:1 zip_code using "`zip10rucafile'", keep(match master) keepusing(zip_ruca_primary_4cat*)

clonevar affluence = affluence13_17
clonevar popden = popden13_17
clonevar disadvantage = disadvantage2_13_17

local myvars "affluence disadvantage"
local numcats = 4
foreach myvar of local myvars {
    * make US quantiles
	xtile `myvar'_`numcats'tile = `myvar', nquantiles(`numcats')
	* make State quantiles
	egen `myvar'_state_`numcats'tile = xtile(`myvar'), n(`numcats') by(state_fips)	
}

replace zip_pop = round(((cases/cases_per_10k)*10000),1) if missing(zip_pop)
*clonevar population = zip_pop

tab1 state if zip_pop != totpop13_17 & !missing(zip_pop)

* make zcta level variables
egen zcta_cases = total(cases), by(zcta state_fips) missing

* only good for states that provided a population 
egen zcta_tot_zip_pop = total(zip_pop) if inlist(state, "IN", "ME", "NC", "OH", "OR", "RI"), by(zcta state_fips) missing

gen zcta_pop = totpop13_17 if !inlist(state, "IN", "ME", "NC", "OH", "OR", "RI")
replace zcta_pop = zcta_tot_zip_pop if inlist(state, "IN", "ME", "NC", "OH", "OR", "RI")

gen zcta_cases_per_10k = zcta_cases / (zcta_pop/10000)
gen zcta_cases_per_100k = zcta_cases_per_10k * 10

sort state zcta zip_code
egen tag_zcta = tag(state zcta)

save "`mergefile'", replace

preserve
local myvars "state_fips stcofips zip_code zip_data has_dates zip_pop cases cases_per_100k tot_state_cases tot_state_cases_per_100k zip_phase1_cases_per_100k zip_phase2_cases_per_100k zip_phase3_cases_per_100k zip_phase4_cases_per_100k hi75_cases_per_100k hi75_zip_phase1_cases_per_100k hi75_zip_phase2_cases_per_100k hi75_zip_phase3_cases_per_100k hi75_zip_phase4_cases_per_100k tot_state_phase1_cases_per_100k tot_state_phase2_cases_per_100k tot_state_phase3_cases_per_100k tot_state_phase4_cases_per_100k popden affluence affluence_state_4tile affluence_4tile disadvantage disadvantage_state_4tile disadvantage_4tile partisan_index_rep zip_ruca_primary_4cat* min_date max_date"
keep `myvars'
order `myvars'

sort state_fips zip_code

save "`mergefile'_subset", replace
restore

* make zcta subset
keep if tag_zcta == 1
local myvars "state_fips stcofips zcta zip_data has_dates zcta_pop zcta_cases zcta_cases_per_10k zcta_cases_per_100k tot_state_cases tot_state_cases_per_100k popden affluence affluence_state_4tile affluence_4tile disadvantage disadvantage_state_4tile disadvantage_4tile partisan_index_rep zip_ruca_primary_4cat* min_date max_date"
keep `myvars'
order `myvars'

summ, format

sort state_fips zcta

save "`mergefile'_zcta_subset", replace

capture log close
