local version "20231001"
local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'process_Delaware_date_`version'.log", replace

local in_data_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/Delaware/DelawareCasesByCensusTract2022/"
local data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
local infile_stub "DelawareCasesByCensusTract2022"
local infile "`in_data_dir'`infile_stub'.csv"
local outfile "`data_dir'`infile_stub'_date_`version'.dta"
local outfile_subset "`data_dir'`infile_stub'_date_subset_`version'.dta"
local censusdata "D:\Census\ACS\2018\data\Tracts_Block_Groups_Only\sfe_all_us_stack_tract_ses_sub_v2.dta"


cd "`data_dir'"
import delimited "`infile'", varnames(1) asdouble clear 
browse
codebook
help destring
destring value, gen(value_num) force
browse if missing(value_num)
tab1 statistic

capture drop date_str
gen date_str = month + "/" + day + "/" + year

*capture drop date
gen date = date(date_str, "MDY", 2050)
browse
format %td date
browse

gen state_fips = "10"
gen county_fips = ""
replace county_fips="001" if county=="Kent County"
replace county_fips="003" if county=="New Castle County"
replace county_fips="005" if county=="Sussex County"

capture drop tract_num
gen tract_num = real(regexs(1))*100 if regexm(location, "([0-9.]+)")

clonevar COUNTYFP10 = county_fips
clonevar NAMELSAD10 = location

merge m:1 COUNTYFP10 NAMELSAD10 using "D:/Census/tiger/TIGER2010/TRACT/2010/tl_2010_10_tract10.dta", keep(master match) gen(_merge_shp)
tab1 location if _merge_shp==1
clonevar tract_fips10 = GEOID10
replace tract_fips10 = state_fips + county_fips + "050104" if  _merge_shp==1 & location=="Census Tract 501.04"

merge m:1 tract_fips10 using "D:\Census\ACS\2018\data\Tracts_Block_Groups_Only\sfe_all_us_stack_tract_ses_sub_v2.dta", keep(master match) gen(_merge_acs2018)

* positive cases = confirmed cases + probable cases
* should I use cumulative or not?

*positive always (almost always?) > cumulative
egen max_date = max(date), by(tract_fips10)
format %td max_date
egen max_pos_cases_10k = max(value_num) if statistic=="Positive Cases" & unit=="rate per 10,000 people", by(tract_fips10)
tab1 max_date

egen min_date = min(date), by(tract_fips10)
format %td min_date


gen diff_max_pos_cases_date = max_pos_cases_10k - value_num if statistic=="Positive Cases" & unit=="rate per 10,000 people" & date==max_date

gen pre_tract_date_cases = value_num if statistic=="Cumulative Number of Positive Cases" & unit=="people"
egen tract_date_cases = max(pre_tract_date_cases), by(tract_fips10 date)

gen tract_cases_per_10k = value_num if statistic=="Positive Cases" & unit=="rate per 10,000 people"
label var tract_cases_per_10k "Positive COVID-19 case rate per 10,000 people"

* make phase variable
label define phasef 1 "April-May 2020" 2 "June-August 2020" 3 "September-December 2020" 4 "Beyond December 2020"
gen phase = .
replace phase = 1 if date >= td(01jan2020) & date < td(01jun2020)
replace phase = 2 if date >= td(01jun2020) & date < td(01sep2020)
replace phase = 3 if date >= td(01sep2020) & date < td(01jan2021)
replace phase = 4 if date >= td(01jan2021) & date < td(01may2022)
label values phase phasef
tab1 phase
bigtab phase date

sort tract_fips10 date
egen max_tract_phase_date = max(date) if statistic=="Positive Cases" & unit=="rate per 10,000 people", by(tract_fips10 phase) 
format %td max_tract_phase_date

sort tract_fips10 date
gen tag_last_tract_date = .
replace tag_last_tract_date = 1 if date == max_tract_phase_date & statistic=="Positive Cases" & unit=="rate per 10,000 people"


gen pre_tract_phase1_cases_per_10k = tract_cases_per_10k if phase==1 & date==max_tract_phase_date 
gen pre_tract_phase2_cases_per_10k = tract_cases_per_10k if phase==2 & date==max_tract_phase_date
gen pre_tract_phase3_cases_per_10k = tract_cases_per_10k if phase==3 & date==max_tract_phase_date
gen pre_tract_phase4_cases_per_10k = tract_cases_per_10k if phase==4 & date==max_tract_phase_date

* Subtraction
egen tract_phase1_cuml_cases_per_10k = max(pre_tract_phase1_cases_per_10k), by(tract_fips10)
egen tract_phase2_cuml_cases_per_10k = max(pre_tract_phase2_cases_per_10k), by(tract_fips10)
egen tract_phase3_cuml_cases_per_10k = max(pre_tract_phase3_cases_per_10k), by(tract_fips10)
egen tract_phase4_cuml_cases_per_10k = max(pre_tract_phase4_cases_per_10k), by(tract_fips10)

gen tract_phase1_cases_per_10k = tract_phase1_cuml_cases_per_10k
gen tract_phase2_cases_per_10k = tract_phase2_cuml_cases_per_10k - tract_phase1_cuml_cases_per_10k
gen tract_phase3_cases_per_10k = tract_phase3_cuml_cases_per_10k - tract_phase2_cuml_cases_per_10k
gen tract_phase4_cases_per_10k = tract_phase4_cuml_cases_per_10k - tract_phase3_cuml_cases_per_10k

gen tract_phase1_cases_per_100k = tract_phase1_cases_per_10k * 10
gen tract_phase2_cases_per_100k = tract_phase2_cases_per_10k * 10
gen tract_phase3_cases_per_100k = tract_phase3_cases_per_10k * 10
gen tract_phase4_cases_per_100k = tract_phase4_cases_per_10k * 10

egen pre_state_phase1_cases = total(tract_date_cases) if phase==1 & date==max_tract_phase_date & statistic=="Positive Cases" & unit=="rate per 10,000 people", by(state_fips)
egen pre_state_phase2_cases = total(tract_date_cases) if phase==2 & date==max_tract_phase_date & statistic=="Positive Cases" & unit=="rate per 10,000 people", by(state_fips)
egen pre_state_phase3_cases = total(tract_date_cases) if phase==3 & date==max_tract_phase_date & statistic=="Positive Cases" & unit=="rate per 10,000 people", by(state_fips)
egen pre_state_phase4_cases = total(tract_date_cases) if phase==4 & date==max_tract_phase_date & statistic=="Positive Cases" & unit=="rate per 10,000 people", by(state_fips)

egen state_phase1_cases = max(pre_state_phase1_cases), by(state_fips)
egen state_phase2_cases = max(pre_state_phase2_cases), by(state_fips)
egen state_phase3_cases = max(pre_state_phase3_cases), by(state_fips)
egen state_phase4_cases = max(pre_state_phase4_cases), by(state_fips)

gen state_phase1_cases_per_10k = state_phase1_cases / (state_totpop18/10000)
gen state_phase2_cases_per_10k = state_phase2_cases / (state_totpop18/10000)
gen state_phase3_cases_per_10k = state_phase3_cases / (state_totpop18/10000)
gen state_phase4_cases_per_10k = state_phase4_cases / (state_totpop18/10000)

gen state_phase1_cases_per_100k = state_phase1_cases_per_10k * 10
gen state_phase2_cases_per_100k = state_phase2_cases_per_10k * 10
gen state_phase3_cases_per_100k = state_phase3_cases_per_10k * 10
gen state_phase4_cases_per_100k = state_phase4_cases_per_10k * 10

egen pre_state_total_cases = total(tract_date_cases) if date==max_date & statistic=="Positive Cases" & unit=="rate per 10,000 people", by(state_fips)
egen state_total_cases = max(pre_state_total_cases), by(state_fips)
gen state_total_cases_per_10k = state_total_cases / (state_totpop18/10000)

gen state_total_cases_per_100k = state_total_cases_per_10k * 10

gen pre_tract_total_cases_per_10k = tract_cases_per_10k if date==max_date & statistic=="Positive Cases" & unit=="rate per 10,000 people"
egen tract_total_cases_per_10k = max(pre_tract_total_cases_per_10k), by(tract_fips10)

gen tract_total_cases_per_100k = tract_total_cases_per_10k * 10

save "`outfile'", replace

* make subset of one row per tract of positive cases per 10k on last day of data
keep if statistic=="Positive Cases" & unit=="rate per 10,000 people" & date==max_date



* make a subset with just analysis vars
keep state_fips tract_fips10 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k min_date max_date

order state_fips tract_fips10 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k min_date max_date

sort state_fips

save "`outfile_subset'", replace

codebook
summ , format

capture log close


