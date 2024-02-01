local version "20231012"
local in_data_version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'combine_zips_tracts_make_tables_`version'.log", replace

/* Cluster by county */

local state_names "Arizona Florida Illinois Indiana Maine Maryland Minnesota NorthCarolina Nevada NewYork Ohio Oklahoma Oregon Pennsylvania RhodeIsland Vermont Virginia"
local state_fips_codes "04 12 17 18 23 24 27 37 32 36 39 40 41 42 44 50 51"
* states with dates available
local state_date_names "Maryland Minnesota Nevada NewYork Vermont Virginia"
local out_data_dir "D:/covid/Noppert/data/Covid-19Data/"
local data_dir "D:/covid/Noppert/data/Covid-19Data/ZipcodeLevel/"
local tract_data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
*local infile_stubs ""
local outfile "`data_dir'covid_all_states_zipcode_dates_`in_data_version'"
local tractoutfile "`tract_data_dir'covid_all_states_tracts_dates_`in_data_version'"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zip_county_xwalk "D:/covid/Noppert/data/best_zip_county_062020.dta"
local censusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"
local statepop17file "D:/Census/ACS/2017/2013-2017/data/Tracts_Block_Groups_Only/state_pop_nanda_ses_tract_2008-2017_04.dta"
local statepop18file "D:/Census/ACS/2018/data/Tracts_Block_Groups_Only/state_pop18_sfe_all_us_stack_tract_ses_sub_v2.dta"
local statepop19file "D:/Census/ACS/2019/data/Tracts_Block_Groups_Only/state_pop19_sfe_all_us_ses_vars_tract.dta"
local statepop20file "D:/Census/ACS/2020/data/Tracts_Block_Groups_Only/state_fips_pop_acs_2016_2020_tract_ses_subset.dta"
local politicalfile "O:/NaNDA/Data/voting/voting_county_2004-2018/datasets/nanda_voting_county_2018_01P"
local mergefile "`outfile'_merge_ses"
local zipanalysisfile "`outfile'_merge_ses_zcta_subset"
local tractanalysisfile "`tractoutfile'_merge_ses_subset"
local ziptractanalysisfile "`out_data_dir'covid_all_states_zipcode_tract_dates_`version'_merge_ses_zcta_subset"
local statecodesfile "D:/Census/state_codes"
local essocctractfile "D:\Census\ACS\2019\data\Tracts_Block_Groups_Only\sfe_all_us_ses_vars_tract_essoccvars_v`version'.dta"
local essocczctafile "D:\Census\ACS\2019\data\All_Geographies_Not_Tracts_Block_Groups\sfe_all_us_rev_zcta_essoccvars_v`version'.dta" 
local zip10rucafile "D:/Census/ruca/data/zip_2010_ruca_4cat"
local tract10rucafile "D:/Census/ruca/data/tract10_2010_ruca_4cat"

cd "`data_dir'"

* Combine zip data and tract data
use "`zipanalysisfile'", clear
append using "`tractanalysisfile'", force
save "`ziptractanalysisfile'", replace

* Get county from tract data
replace stcofips = substr(tract_fips10, 1, 5) if !missing(tract_fips10)
replace stcofips = substr(tract_fips20, 1, 5) if !missing(tract_fips20)

* merge in state codes
merge m:1 state_fips using "`statecodesfile'", keep(match master)

* combine tract data with zip data
clonevar tract_cases = tract_total_cases
gen cases = zcta_cases if zip_data==1
replace cases = tract_cases if missing(cases) & tract_data==1
replace cases = round(cases, 1)

gen population = zcta_pop if zip_data==1
replace population = tract_pop if missing(population) & tract_data==1

gen cases_per_100k = zcta_cases_per_100k if zip_data==1
replace cases_per_100k = tract_total_cases_per_100k if missing(cases_per_100k)

replace state_total_cases_per_100k = tot_state_cases_per_100k if missing(state_total_cases_per_100k)


* change to cases per 10k
gen cases_per_10k = cases_per_100k / 10
replace state_total_cases_per_10k = state_total_cases_per_100k / 10 if missing(state_total_cases_per_10k)

* fix outliers
replace cases_per_10k = . if cases_per_10k > 10000
replace cases_per_100k = . if cases_per_100k > 100000
replace state_total_cases_per_10k = . if state_total_cases_per_10k > 10000
replace state_total_cases_per_100k = . if state_total_cases_per_100k > 100000
replace cases = . if missing(cases_per_10k)

* need state cases for tract level data
egen state_total_tract_cases = total(tract_cases), by(state_fips)
replace tot_state_cases = state_total_tract_cases if missing(tot_state_cases) & !missing(state_total_tract_cases)
format tot_state_cases %12.0gc

* make dummies of affluence and disadvantage quartiles
tab affluence_state_4tile, gen(affluence_state_4tile)
tab disadvantage_state_4tile, gen(disadvantage_state_4tile)

* rescale partisan index from proportion to percent for interpretability
gen partisan_index_rep_pct = partisan_index_rep * 100
label var partisan_index_rep_pct "Republican partisanship index (Percent votes cast, past 6 years)"

gen partisan_index_rep_dec = partisan_index_rep_pct / 10
label var partisan_index_rep_dec "Republican partisanship index (Percent votes cast / 10, past 6 years)"

fsum cases_per_10k cases_per_100k state_total_cases_per_10k state_total_cases_per_100k affluence_state_4tile affluence_4tile disadvantage_state_4tile partisan_index_rep partisan_index_rep_pct partisan_index_rep_dec, format(12.4)

* make a variable for geographic regions
label define regionf 1 "Northeast" 2 "Southwest" 3 "West" 4 "Southeast" 5 "Midwest"
gen region = 1 if inlist(state_postal, "DE", "ME", "MD", "NY", "PA", "RI", "VT")
replace region = 2 if inlist(state_postal, "AZ", "NM", "OK")
replace region = 3 if inlist(state_postal, "NV", "OR")
replace region = 4 if inlist(state_postal, "FL", "LA", "NC", "VA")
replace region = 5 if inlist(state_postal, "IL", "IN", "MN", "OH", "WI")
label values region regionf
tab1 region
bigtab region state_postal state_name

* add RUCA codes
count
merge m:1 tract_fips10 using "`tract10rucafile'", keep(match master) keepusing(tract10_ruca_primary_4cat*) nogen
count
gen ruca_primary_4cat = zip_ruca_primary_4cat
replace ruca_primary_4cat = tract10_ruca_primary_4cat if missing(ruca_primary_4cat)
tab ruca_primary_4cat, gen(ruca_primary_4cat)

save "`ziptractanalysisfile'", replace

* make box and whisker plot of tract or zip case by state and region 
label var cases_per_10k "Cases per 10,000 population"
graph hbox cases_per_10k, over(state_postal, label(labsize(vsmall))) over(region, label(labsize(small))) nooutside note("") nofill 
graph export "D:\covid\Noppert\scripts\region_state_covid_hbox_v1_`version'.png", as(png) name("Graph") replace 

capture log close

*** State Level Descriptives of cases per 10k ***
capture log close
log using covid_table1_`version'.log, replace

*use "`ziptractanalysisfile'", clear

file open fh using covid_table1_`version'.html, write replace
file write fh "<html><body><table>"
file write fh _n "<tr><td>CONEP Preliminary Analyses</td></tr>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><b>Table 1</b></tr>"
file write fh _n "<tr><td></td><td><b>State-Wide Trends</b></td><td></td><td></td></tr>"
file write fh _n "<tr><td></td><td></td><td><b>Cumulative Case Count per 10,000 Population</b></td><td><b>Data Collected Through:</b></td></tr>"

local state_names `" "Delaware" "Maine" "Maryland" "New York" "Pennsylvania" "Rhode Island" "Vermont" "Arizona" "New Mexico" "Oklahoma" "Nevada" "Oregon" "Florida" "Louisiana" "North Carolina" "Virginia" "Illinois" "Indiana" "Minnesota" "Ohio" "Wisconsin" "'
local state_fips_codes "10 23 24 36 42 44 50 04 35 40 32 41 12 22 37 51 17 18 27 39 55"

local num_states : word count `state_names'
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
	local state_fips_code : word `i' of `state_fips_codes'
	file write fh _n "<tr>"
    
	file write fh _n "<td>`state_name'</td><td></td>"
	summ state_total_cases_per_10k if state_fips == "`state_fips_code'"
	file write fh _n "<td>" %8.2gc (round(r(mean),.01)) "</td>"
	summ max_date if state_fips == "`state_fips_code'"
	file write fh _n "<td>" %tdnn/dd/CCYY (r(max)) "</td>"

	file write fh _n "</tr>"
}

file write fh _n "</table>"
file write fh _n "</body>"
file write fh _n "</html>"

file close fh
type covid_table1_`version'.html

capture log close

*** State Level Descriptives of cases ***
capture log close
log using covid_table1_cases_`version'.log, replace

*use "`ziptractanalysisfile'", clear

file open fh using covid_table1_cases_`version'.html, write replace
file write fh "<html><body><table>"
file write fh _n "<tr><td>CONEP Preliminary Analyses</td></tr>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><b>Table 1</b></tr>"
file write fh _n "<tr><td></td><td><b>State-Wide Trends</b></td><td></td><td></td></tr>"
file write fh _n "<tr><td></td><td></td><td><b>Cumulative Case Count</b></td><td><b>Data Collected Through:</b></td></tr>"

local state_names `" "Delaware" "Maine" "Maryland" "New York" "Pennsylvania" "Rhode Island" "Vermont" "Arizona" "New Mexico" "Oklahoma" "Nevada" "Oregon" "Florida" "Louisiana" "North Carolina" "Virginia" "Illinois" "Indiana" "Minnesota" "Ohio" "Wisconsin" "'
local state_fips_codes "10 23 24 36 42 44 50 04 35 40 32 41 12 22 37 51 17 18 27 39 55"

local num_states : word count `state_names'
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
	local state_fips_code : word `i' of `state_fips_codes'
	file write fh _n "<tr>"
    
	file write fh _n "<td>`state_name'</td><td></td>"
	summ tot_state_cases if state_fips == "`state_fips_code'"
	file write fh _n "<td>" %12.0gc (round(r(mean),.01)) "</td>"
	summ max_date if state_fips == "`state_fips_code'"
	file write fh _n "<td>" %tdnn/dd/CCYY (r(max)) "</td>"

	file write fh _n "</tr>"
}

file write fh _n "</table>"
file write fh _n "</body>"
file write fh _n "</html>"

file close fh
type covid_table1_cases_`version'.html

capture log close

*** Tract and ZIP level descriptives by State ***
capture log close
log using covid_table2_`version'.log, replace

*use "`ziptractanalysisfile'", clear

file open fh using covid_table2_`version'.html, write replace
file write fh "<html><body><table>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><b>Table 2</b></tr>"
file write fh _n "<tr>"
file write fh _n "<td></td>"

local state_names `" "Delaware" "Maine" "Maryland" "New York" "Pennsylvania" "Rhode Island" "Vermont" "Arizona" "New Mexico" "Oklahoma" "Nevada" "Oregon" "Florida" "Louisiana" "North Carolina" "Virginia" "Illinois" "Indiana" "Minnesota" "Ohio" "Wisconsin" "'
local state_fips_codes "10 23 24 36 42 44 50 04 35 40 32 41 12 22 37 51 17 18 27 39 55"
local tract_states "Delaware Louisiana New Mexico Wisconsin"

local num_states : word count `state_names'
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
	
    file write fh _n "<td style='text-align: center;'>`state_name'</td>"
}

file write fh _n "</tr>"
file write fh _n "<tr>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
	if strpos("`tract_states'", "`state_name'") > 0 {
		file write fh _n "<td style='text-align: center;'>Census tract</td>"
	}
	else {
		file write fh _n "<td style='text-align: center;'>Zip code</td>"
	}
}
file write fh _n "</tr>"

*file write fh _n "</tr>"
file write fh _n "<tr>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td style='text-align: center;'>Median (CV)</td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td><b>Median ZIP Code Case Count Per 10,000 Population</b></td>"
forvalues i=1/`num_states' {
    local state_fips_code : word `i' of `state_fips_codes'
	summ cases_per_10k if state_fips == "`state_fips_code'", detail
    file write fh _n "<td>" %8.2fc (round(r(p50),.01)) " (" (round(r(sd)/r(mean),.01)) ")" "</td>"
}
file write fh _n "</tr>"


file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td><b>Neighborhood Characteristic</b></td></tr>"
file write fh _n "<tr>"
file write fh _n "<td>Affluence</td>"
forvalues i=1/`num_states' {
    local state_fips_code : word `i' of `state_fips_codes'
	summ affluence if state_fips == "`state_fips_code'", detail
    file write fh _n "<td>" (round(r(p50),.01)) " (" (round(r(sd)/r(mean),.01)) ")" "</td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td>Population Density</td>"
forvalues i=1/`num_states' {
    local state_fips_code : word `i' of `state_fips_codes'
	summ popden if state_fips == "`state_fips_code'", detail
    file write fh _n "<td>" %8.2fc (round(r(p50),.01)) " (" (round(r(sd)/r(mean),.01)) ")" "</td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td>Political Partisanship</td>"
forvalues i=1/`num_states' {
    local state_fips_code : word `i' of `state_fips_codes'
	summ partisan_index_rep_dec if state_fips == "`state_fips_code'", detail
    file write fh _n "<td>" (round(r(p50),.01)) " (" (round(r(sd)/r(mean),.01)) ")" "</td>"
}
file write fh _n "</tr>"

file write fh _n "<tr><td>&nbsp;</td></tr>"
 
file write fh _n "</table>"
file write fh _n "</body>"
file write fh _n "</html>"

file close fh
type covid_table2_`version'.html

capture log close

*********************************************
************** Poisson, irr *****************
*********************************************

********************************************************************************
********************************************************************************
********************************************************************************

capture log close
log using covid_table3_poisson_irr_univariates_`version'.log, replace

*use "`ziptractanalysisfile'", clear
capture file close fh
file open fh using covid_table3_poisson_irr_univariates_`version'.html, write replace
file write fh "<html><body><table>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td><b>Table 3A: Cumulative Case Counts</b></td></tr>"
file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"

local state_names `" "Arizona" "Delaware" "Florida" "Illinois" "Indiana" "Louisiana" "Maine" "Maryland" "Minnesota" "New Mexico" "North Carolina" "Nevada" "New York" "Ohio" "Oklahoma" "Oregon" "Pennsylvania" "Rhode Island" "Vermont" "Virginia" "Wisconsin" "'
local state_fips_codes "04 10 12 17 18 22 23 24 27 35 37 32 36 39 40 41 42 44 50 51 55"
local num_states : word count `state_names'
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td colspan='1' style='text-align: center;'><b>`state_name'</b></td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td colspan='1' style='text-align: center;'><b>IRR [95% CI]</b></td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td colspan='1' style='text-align: center;'>Cases</td>"
}
file write fh _n "</tr>"

/*
file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
	file write fh _n "<td style='text-align: center;'>Model 1</td><td style='text-align: center;'>Model 2</td>"
}
file write fh _n "</tr>"
*/

file write fh _n "<tr>"
file write fh _n "<td><b>Neighborhood Characteristics</b></td>"
file write fh _n "</tr>"

local dv "cases"
local ivs "NULL NULL NULL affluence_state_4tile2 affluence_state_4tile3 affluence_state_4tile4 NULL NULL disadvantage_state_4tile2 disadvantage_state_4tile3 disadvantage_state_4tile4 NULL popden partisan_index_rep_dec NULL NULL ruca_primary_4cat2 ruca_primary_4cat3 ruca_primary_4cat4"
*local ivs "disadvantage popden partisan_index_rep_dec"
local rowlabels `" "<b>Neighborhood Characteristic</b>" "Neighborhood Affluence" "Q1 (ref)" "Q2" "Q3" "Q4 (Highest Affluence)" "Neighborhood Disadvantage" "Q1 (ref)" "Q2" "Q3" "Q4 (Highest Disadvantage)" "&nbsp;"  "Neighborhood Population Density" "County-Level Political Partisanship" "<b>Rural-Urban Commuting Area Codes</b>" "Metropolitan (ref)" "Micropolitan" "Small town" "Rural" "'
*local rowlabels `" "Neighborhood Disadvantage" "Neighborhood Population Density" "County-Level Political Partisanship" "'
local numrows : word count `rowlabels'
local models `" "poisson `dv' affluence_state_4tile2 affluence_state_4tile3 affluence_state_4tile4" "poisson `dv' disadvantage_state_4tile2 disadvantage_state_4tile3 disadvantage_state_4tile4" "poisson `dv' popden" "poisson `dv' partisan_index_rep_dec" "poisson `dv' ruca_primary_4cat2 ruca_primary_4cat3 ruca_primary_4cat4" "'
local nummodels : word count `models'

forvalues i=1/`numrows' {
    local rowlabel : word `i' of `rowlabels'
	local iv : word `i' of `ivs'
	display "`rowlabel'   `iv'"
	
	*print the row label
	file write fh _n "<tr>"
	file write fh _n "<td>`rowlabel'</td>"
	file write fh _n "<td>&nbsp;</td>"
	
	local b "&nbsp;"
	local lb95 "&nbsp;"
	local ub95 "&nbsp;"
	local ci95 "&nbsp;"
	local z "&nbsp;"
	local p "&nbsp;"
	local stars "&nbsp;"


			
	forvalues k=1/`nummodels'	{
		local model : word `k' of `models'
		* initialize cell contents
		ereturn clear
		local b "&nbsp;"
		local lb95 "&nbsp;"
		local ub95 "&nbsp;"
		local ci95 "&nbsp;"
		local z "&nbsp;"
		local p "&nbsp;"
		local stars "&nbsp;"
		local rho "&nbsp;"
			
		forvalues j=1/`num_states' {
			local state_name : word `j' of `state_names'
			local state_fips_code : word `j' of `state_fips_codes'
		
			display "`rowlabel'   `iv'"
			display "`state_name' `state_fips_code'"
		
			local b "&nbsp;"
			local lb95 "&nbsp;"
			local ub95 "&nbsp;"
			local ci95 "&nbsp;"
			local z "&nbsp;"
			local p "&nbsp;"
			local stars "&nbsp;"			
			
			*only run model if the row variable is in the model
			if strpos("`model'", "`iv'") > 0 {
				if ("`state_fips_code'" == "35" & strpos("`iv'", "ruca") > 0) {
					display "Skip `state_fips_code' `iv'"
				}
				else {
					`model' if state_fips == "`state_fips_code'" , exposure(population) irr cluster(stcofips)
					local b = round(exp(_b[`iv']), .01)
					local lb95 = round(exp(_b[`iv']-(1.96*_se[`iv'])), .01)
					local ub95 = round(exp(_b[`iv']+(1.96*_se[`iv'])), .01)
					local ci95 "[`lb95'-`ub95']"
					local z =  _b[`iv']/_se[`iv']
					local p = 2*normal(-abs(`z'))
					*display "`p'"
					
					/*
					pwcorr cases_per_100k `iv' if state_fips == "`state_fips_code'", sig
					local rho = r(rho)
					local rho = round(`rho',.0001)
					local p = r(sig)[1,2]
					*/
						
					local stars ""
					if `p' < 0.001 {
						local stars "***"
					}
					else if `p' < 0.01 {
						local stars "**"
					}
					else if `p' < 0.05 {
						local stars "*"
					}
					else if `p' < 0.1 {
						local stars "+"
					}
					*display "`stars'"
				
				} /* end else */
				file write fh _n "<td style='text-align:left;'>`b'`stars' `ci95'</td>"
			}
			*file write fh _n "<td style='text-align:left;'>`b'`stars' `ci95'</td>"
		}
	}
	file write fh _n "</tr>"
}

file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td>+ p&lt;0.10, * p&lt;0.05, ** p&lt;0.01, *** p&lt;0.001</td></tr>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td>&nbsp;</td></tr>"

file write fh _n "</table>"
file write fh _n "</body>"
file write fh _n "</html>"

file close fh
type covid_table3_poisson_irr_univariates_`version'.html

capture log close

********************************************************************************
********************************************************************************
********************************************************************************

capture log close
log using covid_table3_pwcorr_univariates_`version'.log, replace

*use "`ziptractanalysisfile'", clear
capture file close fh
file open fh using covid_table3_pwcorr_univariates_`version'.html, write replace
file write fh "<html><body><table>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td><b>Table 3A: Cumulative Case Counts</b></td></tr>"
file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"

local state_names `" "Arizona" "Delaware" "Florida" "Illinois" "Indiana" "Louisiana" "Maine" "Maryland" "Minnesota" "New Mexico" "North Carolina" "Nevada" "New York" "Ohio" "Oklahoma" "Oregon" "Pennsylvania" "Rhode Island" "Vermont" "Virginia" "Wisconsin" "'
local state_fips_codes "04 10 12 17 18 22 23 24 27 35 37 32 36 39 40 41 42 44 50 51 55"
local num_states : word count `state_names'
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td colspan='1' style='text-align: center;'><b>`state_name'</b></td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td colspan='1' style='text-align: center;'><b>&rho;</b></td>"
}
file write fh _n "</tr>"

file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
    local state_name : word `i' of `state_names'
    file write fh _n "<td colspan='1' style='text-align: center;'>Cases per 10k</td>"
}
file write fh _n "</tr>"

/*
file write fh _n "<tr>"
file write fh _n "<td></td>"
file write fh _n "<td></td>"
forvalues i=1/`num_states' {
	file write fh _n "<td style='text-align: center;'>Model 1</td><td style='text-align: center;'>Model 2</td>"
}
file write fh _n "</tr>"
*/

file write fh _n "<tr>"
file write fh _n "<td><b>Neighborhood Characteristics</b></td>"
file write fh _n "</tr>"

local dv "cases_per_10k"
*local ivs "NULL NULL affluence NULL disadvantage_state_4tile1 disadvantage_state_4tile2 disadvantage_state_4tile3 disadvantage_state_4tile4 NULL popden partisan_index_rep_dec NULL ruca_primary_4cat1 ruca_primary_4cat2 ruca_primary_4cat3 ruca_primary_4cat4"
local ivs "affluence disadvantage popden partisan_index_rep_dec ruca_primary_4cat"
*local rowlabels `" "<b>Neighborhood Characteristic</b>" "Neighborhood Affluence" "Q1 (ref)" "Q2" "Q3" "Q4 (Highest Affluence)" "Neighborhood Disadvantage" "Q1 (ref)" "Q2" "Q3" "Q4 (Highest Disadvantage)" "&nbsp;"  "Neighborhood Population Density" "County-Level Political Partisanship" "<b>Rural-Urban Commuting Area Codes</b>" "Metropolitan (ref)" "Micropolitan" "Small town" "Rural" "'
local rowlabels `" "Neighborhood Affluence" "Neighborhood Disadvantage" "Neighborhood Population Density" "County-Level Political Partisanship" "RUCA codes: Metropolitan(low)-Rural(high)" "'
local numrows : word count `rowlabels'
local models "affluence disadvantage popden partisan_index_rep_dec ruca_primary_4cat"
local nummodels : word count `models'

forvalues i=1/`numrows' {
    local rowlabel : word `i' of `rowlabels'
	local iv : word `i' of `ivs'
	display "`rowlabel'   `iv'"
	
	*print the row label
	file write fh _n "<tr>"
	file write fh _n "<td>`rowlabel'</td>"
	file write fh _n "<td>&nbsp;</td>"
	
	local b "&nbsp;"
	local lb95 "&nbsp;"
	local ub95 "&nbsp;"
	local ci95 "&nbsp;"
	local z "&nbsp;"
	local p "&nbsp;"
	local stars "&nbsp;"


			
	forvalues k=1/`nummodels'	{
		local model : word `k' of `models'
		* initialize cell contents
		ereturn clear
		local b "&nbsp;"
		local lb95 "&nbsp;"
		local ub95 "&nbsp;"
		local ci95 "&nbsp;"
		local z "&nbsp;"
		local p "&nbsp;"
		local stars "&nbsp;"
		local rho "&nbsp;"
			
		forvalues j=1/`num_states' {
			local state_name : word `j' of `state_names'
			local state_fips_code : word `j' of `state_fips_codes'
		
			display "`rowlabel'   `iv'"
			display "`state_name' `state_fips_code'"
		
			local b "&nbsp;"
			local lb95 "&nbsp;"
			local ub95 "&nbsp;"
			local ci95 "&nbsp;"
			local z "&nbsp;"
			local p "&nbsp;"
			local stars "&nbsp;"			
			local rho "&nbsp;"
			
			*only run model if the row variable is in the model
			if strpos("`model'", "`iv'") > 0 {
				if ("`state_fips_code'" == "35" & strpos("`iv'", "ruca") > 0) {
					display "Skip `state_fips_code' `iv'"
				}
				else {
					/*`model' if state_fips == "`state_fips_code'"
					local b = round(_b[`iv'], .01)
					local lb95 = round(_b[`iv']-(1.96*_se[`iv']), .01)
					local ub95 = round(_b[`iv']+(1.96*_se[`iv']), .01)
					local ci95 "[`lb95'-`ub95']"
					local z =  _b[`iv']/_se[`iv']
					local p = 2*normal(-abs(`z'))
					display `p'
					*/
					pwcorr `dv' `iv' if state_fips == "`state_fips_code'", sig
					local rho = r(rho)
					local rho = round(`rho',.0001)
					local p = r(sig)[1,2]
						
					local stars ""
					if `p' < 0.001 {
						local stars "***"
					}
					else if `p' < 0.01 {
						local stars "**"
					}
					else if `p' < 0.05 {
						local stars "*"
					}
					else if `p' < 0.1 {
						local stars "+"
					}
					*display "`stars'"
				
				} /* end else */
				file write fh _n "<td style='text-align:left;'>`rho'`stars'</td>"
			}
			*file write fh _n "<td style='text-align:left;'>`b'`stars' `ci95'</td>"
		}
	}
	file write fh _n "</tr>"
}

file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td>+ p&lt;0.10, * p&lt;0.05, ** p&lt;0.01, *** p&lt;0.001</td></tr>"
file write fh _n "<tr><td>&nbsp;</td></tr>"
file write fh _n "<tr><td>&nbsp;</td></tr>"

file write fh _n "</table>"
file write fh _n "</body>"
file write fh _n "</html>"

file close fh
type covid_table3_pwcorr_univariates_`version'.html

capture log close

********************************************************************************
********************************************************************************
********************************************************************************


