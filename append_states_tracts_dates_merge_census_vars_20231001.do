local script_dir "D:/covid/Noppert/scripts/"
local version "20231001"
capture log close
log using "`script_dir'append_states_tract_dates_merge_census_vars_`version'.log", replace

* winsconsin zip has dates 
local state_names "Delaware Louisiana NewMexico RhodeIsland Wisconsin"
* states with dates available
local state_date_names "Delaware Louisiana NewMexico Wisconsin"
local data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
*local infile_stubs ""
local outfile "`data_dir'covid_all_states_tracts_dates_`version'"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zip_county_xwalk "D:/covid/Noppert/data/best_zip_county_062020.dta"
local censusfile0817 "D:/Census/ACS/2017/2013-2017/data/Tracts_Block_Groups_Only/nanda_ses_tract_2008-2017_04.dta"
local censusfile1620 "D:/Census/ACS/2020/data/Tracts_Block_Groups_Only/acs_2016_2020_tract_ses_subset.dta"
local censusfile1519 "D:/Census/ACS/2019/data/Tracts_Block_Groups_Only/sfe_all_us_ses_vars_tract.dta"
local statepop17file "D:/Census/ACS/2017/2013-2017/data/Tracts_Block_Groups_Only/state_pop_nanda_ses_tract_2008-2017_04.dta"
local statepop19file "D:\Census\ACS\2019\data\Tracts_Block_Groups_Only\state_pop19_sfe_all_us_ses_vars_tract.dta"
local tractshpfile20 "D:/Census/tiger/TIGER2020/TRACT/tl_2020_US_tract.dta"
local wispopfile "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/WisconsinCasesByCensusTract2022"
local politicalfile "O:/NaNDA/Data/voting/voting_county_2004-2018/datasets/nanda_voting_county_2018_01P"
local mergefile "`outfile'_merge_ses"

cd "`data_dir'"

* loop through state names
local num_files : word count `state_names'
forvalues i=1/`num_files' {

	display "`i'"
	local state_name : word `i' of `state_names'
	
	* filenames change if has dates
	local infile_stub "`state_name'_covid_tract_date_subset_`version'"
	capture confirm file "`infile_stub'.dta"
	if _rc==0 {
		display "Found date file `infile_stub' for `state_name', proceed"
	}
	else {
		display "Did not find date file `infile_stub' for `state_name', try no date file"
		local infile_stub "`state_name'_covid_tract_subset_`version'.dta"
	}	
	
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

* flag as tract data
gen tract_data = 1

* flag states with dates
gen has_dates = 0
replace has_dates = 1 if inlist(state_fips, "10", "22", "35")

* combine tract id's
gen geoid = tract_fips10
replace geoid = tract_fips20 if state_fips=="35"

* get state-county fips from tract fips
gen stcofips = substr(geoid, 1, 5)

* tag duplicate tract ids
duplicates tag geoid, gen(tag_dup_geoid)
tab1 tag_dup_geoid

* merge in Census vars (2010 geometry) ACS 2008-2012 and ACS 2013-2017
merge m:1 tract_fips10 using "`censusfile0817'", keep(master match) gen(_merge_ses0817)

* merge in Census vars (2020 geometry) ACS 2016-2020
merge m:1 tract_fips20 using "`censusfile1620'", keep(master match) gen(_merge_ses1620)

* merge in shapefile to get area
gen GEOID = tract_fips20
merge m:1 GEOID using "`tractshpfile20'", keep(master match) gen(_merge_tractshp20)
* ALAND is in sq meters, convert to square miles
gen aland20 = ALAND / 2589988.11
label var aland20 "Census Tract Land Area in square miles" 

* make population density
gen popden20 = totpop20 / aland20

* combine 13_17 and 16_20
local myvars "affluence disadvantage2_ popden"
foreach myvar of local myvars {
    gen `myvar' = `myvar'13_17
	replace `myvar' = `myvar'20 if missing(`myvar')
}
clonevar disadvantage = disadvantage2_

* make quantiles of these variables
local numcats "4"
local myvars "tract_total_cases_per_100k tract_phase1_cases_per_100k tract_phase2_cases_per_100k tract_phase3_cases_per_100k tract_phase4_cases_per_100k"
foreach myvar of local myvars {
	egen `myvar'_`numcats'c = xtile(`myvar'), n(`numcats') by(state_fips)
	tab `myvar'_`numcats'c, gen(`myvar'_`numcats'c)
	gen hi75_`myvar' = `myvar'_`numcats'c4
	bigtab hi75_`myvar' `myvar'_`numcats'c `myvar'	
}

* merge in political partisanship
merge m:1 stcofips using "`politicalfile'", keep(master match) gen(_merge_political)
fsum partisan_index_rep

* make quantiles of these variables
local myvars "affluence disadvantage"
local numcats = 4
foreach myvar of local myvars {
    * make US quantiles
	xtile `myvar'_`numcats'tile = `myvar', nquantiles(`numcats')
	* make State quantiles
	egen `myvar'_state_`numcats'tile = xtile(`myvar'), n(`numcats') by(state_fips)	
}

* merge in Census vars (2010 geometry) ACS 2015-2019
merge m:1 tract_fips10 using "`censusfile1519'", keep(master match) keepusing(totpop) gen(_merge_ses1519)
rename totpop totpop19

* merge in Population provided with Wisconsin Covid data
merge m:1 tract_fips10 using "`wispopfile'", keepusing(pop) keep(master match) gen(_merge_wis_pop)
rename pop wispop

* merge in state total population from ACS 2013-2017
merge m:1 state_fips using "`statepop17file'", keep(master match) gen(_merge_statepop17)
replace state_pop = state_totpop13_17 if state_fips == "44" & missing(state_pop)

* merge in state total population from ACS 2015-2019, using 2015-2019 to match Delaware data
merge m:1 state_fips using "`statepop19file'", keep(master match) gen(_merge_statepop19)
replace state_pop = state_totpop19 if state_fips == "10" & missing(state_pop)

* combine population vars
capture gen tract_pop = .
replace tract_pop = totpop19 if state_fips == "10" & missing(tract_pop)
replace tract_pop = totpop13_17 if state_fips == "22" & missing(tract_pop)
replace tract_pop = totpop20 if state_fips == "35" & missing(tract_pop)
replace tract_pop = totpop13_17 if state_fips == "44" & missing(tract_pop)
replace tract_pop = wispop if state_fips == "55" & missing(tract_pop)

* get cases for states / tracts that only provide cases per 10k
bysort state_fips: summ tract_cases tract_total_cases
capture gen tract_cases = round((tract_total_cases_per_10k*(tract_pop/10000)),1)
replace tract_cases = round((tract_total_cases_per_10k*(tract_pop/10000)),1) if missing(tract_cases)

* fill in tract cases where missing
replace tract_total_cases = tract_cases if missing(tract_total_cases)
replace tract_total_cases_per_100k = tract_total_cases_per_10k * 10 if missing(tract_total_cases_per_100k)
replace state_total_cases_per_100k = state_total_cases_per_10k * 10 if missing(state_total_cases_per_100k)

* fill in state cases where missing
bysort state_fips: summ tract_cases tract_total_cases state_total_cases
egen _state_cases = total(tract_cases), by(state_fips)
replace state_total_cases = _state_cases if missing(state_total_cases)
bysort state_fips: summ state_total_cases _state_cases

fsum state_total_cases _state_cases , format(16.0)
bysort state_fips: summ state_total_cases _state_cases state_pop state_total_cases_per_10k



save "`mergefile'", replace

local myvars "state_fips geoid tract_fips10 tract_fips20 tract_data has_dates tract_total_cases_per_100k state_total_cases_per_100k tract_phase1_cases_per_100k tract_phase2_cases_per_100k tract_phase3_cases_per_100k tract_phase4_cases_per_100k hi75_tract_total_cases_per_100k hi75_tract_phase1_cases_per_100k hi75_tract_phase2_cases_per_100k hi75_tract_phase3_cases_per_100k hi75_tract_phase4_cases_per_100k state_phase1_cases_per_100k state_phase2_cases_per_100k state_phase3_cases_per_100k state_phase4_cases_per_100k popden affluence affluence_state_4tile affluence_4tile disadvantage disadvantage_state_4tile disadvantage_4tile partisan_index_rep tract_total_cases tract_pop state_total_cases state_total_cases_per_10k state_pop min_date max_date"
keep `myvars'
order `myvars'

summ, format

sort state_fips geoid

save "`mergefile'_subset", replace

capture log close
