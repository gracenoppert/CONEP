local version "20231001"
local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'process_Wisconsin_dates_`version'.log", replace

local state_name "Wisconsin"
local in_data_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/Wisconsin/" 
local data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
local infile_stub "COVID19-Historical-V2-TRCT"
local infile "`in_data_dir'`infile_stub'.csv"
local outfile "`data_dir'WisconsinCensusTract_dates_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_tract_date_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", stringcols(2) asdouble clear 
browse
*codebook

clonevar tract_fips10 = geoid

rename date datetime_str
gen date_str = substr(datetime_str,1,10)

* make date vars
gen date = date(date_str, "MDY", 2050)
format %td date

*POS_CUM_CP: Cumulative total number of cases, either confirmed or probable.
*POS_CUM_CONF: Cumulative total number of confirmed cases.
*POS_CUM_PROB: Cumulative total number of probable cases.

gen cases = pos_cp
label var cases "Positive COVID-19 cases, cumulative"

* merge in population from other Wisconsin file
merge m:1 tract_fips10 using "D:\covid\Noppert\data\Covid-19Data\CensusTractLevel\WisconsinCasesByCensusTract2022", keepusing(pop state_pop) keep(master match) gen(_merge_wis_pop)

clonevar tract_pop = pop

gen cases_per_10k = cases / (pop/10000)
label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen cases_per_100k = cases_per_10k * 10
label var cases_per_100k "Positive COVID-19 case rate per 100,000 people"

clonevar tract_cases_per_10k = cases_per_10k
clonevar tract_cases_per_100k = cases_per_100k

gen state_fips = substr(tract_fips10, 1, 2)

/*
egen state_pop = total(pop), by(state_fips)
egen state_cases = total(cases), by(state_fips)
gen state_total_cases_per_10k = state_cases / (state_pop/10000)
gen state_total_cases_per_100k = state_total_cases_per_10k * 10
*/

* make phase variable
label define phasef 1 "April-May 2020" 2 "June-August 2020" 3 "September-December 2020" 4 "Beyond December 2020"
gen phase = .
replace phase = 1 if date >= td(01jan2020) & date < td(01jun2020)
replace phase = 2 if date >= td(01jun2020) & date < td(01sep2020)
replace phase = 3 if date >= td(01sep2020) & date < td(01jan2021)
replace phase = 4 if date >= td(01jan2021) & !missing(date)
label values phase phasef
tab1 phase
bigtab phase date

sort tract_fips10 date
egen max_tract_phase_date = max(date), by(tract_fips10 phase) 
format %td max_tract_phase_date
bigtab phase max_tract_phase_date

sort tract_fips10 date
egen max_tract_date = max(date), by(tract_fips10) 
format %td max_tract_date
bigtab max_tract_date

egen min_date = min(date), by(tract_fips10)
format %td min_date

egen max_date = max(date), by(tract_fips10)
format %td max_date

summ date min_date max_date, format

sort tract_fips10 date
gen tag_last_tract_date = .
replace tag_last_tract_date = 1 if date == max_tract_phase_date

* Subtraction
gen _tract_phase1_cuml_cases_per_10k = tract_cases_per_10k if phase==1 & tag_last_tract_date==1
gen _tract_phase2_cuml_cases_per_10k = tract_cases_per_10k if phase==2 & tag_last_tract_date==1
gen _tract_phase3_cuml_cases_per_10k = tract_cases_per_10k if phase==3 & tag_last_tract_date==1
gen _tract_phase4_cuml_cases_per_10k = tract_cases_per_10k if phase==4 & tag_last_tract_date==1

egen tract_phase1_cuml_cases_per_10k = max(_tract_phase1_cuml_cases_per_10k), by(tract_fips10)
egen tract_phase2_cuml_cases_per_10k = max(_tract_phase2_cuml_cases_per_10k), by(tract_fips10)
egen tract_phase3_cuml_cases_per_10k = max(_tract_phase3_cuml_cases_per_10k), by(tract_fips10)
egen tract_phase4_cuml_cases_per_10k = max(_tract_phase4_cuml_cases_per_10k), by(tract_fips10)

gen tract_phase1_cases_per_10k = tract_phase1_cuml_cases_per_10k
gen tract_phase2_cases_per_10k = tract_phase2_cuml_cases_per_10k - tract_phase1_cuml_cases_per_10k
gen tract_phase3_cases_per_10k = tract_phase3_cuml_cases_per_10k - tract_phase2_cuml_cases_per_10k
gen tract_phase4_cases_per_10k = tract_phase4_cuml_cases_per_10k - tract_phase3_cuml_cases_per_10k

gen tract_phase1_cases_per_100k = tract_phase1_cases_per_10k * 10
gen tract_phase2_cases_per_100k = tract_phase2_cases_per_10k * 10
gen tract_phase3_cases_per_100k = tract_phase3_cases_per_10k * 10
gen tract_phase4_cases_per_100k = tract_phase4_cases_per_10k * 10

egen pre_state_phase1_cases = total(cases) if phase==1 & date==max_tract_phase_date, by(state_fips)
egen pre_state_phase2_cases = total(cases) if phase==2 & date==max_tract_phase_date, by(state_fips)
egen pre_state_phase3_cases = total(cases) if phase==3 & date==max_tract_phase_date, by(state_fips)
egen pre_state_phase4_cases = total(cases) if phase==4 & date==max_tract_phase_date, by(state_fips)

egen state_phase1_cases = max(pre_state_phase1_cases), by(state_fips)
egen state_phase2_cases = max(pre_state_phase2_cases), by(state_fips)
egen state_phase3_cases = max(pre_state_phase3_cases), by(state_fips)
egen state_phase4_cases = max(pre_state_phase4_cases), by(state_fips)

gen state_phase1_cases_per_10k = state_phase1_cases / (state_pop/10000)
gen state_phase2_cases_per_10k = state_phase2_cases / (state_pop/10000)
gen state_phase3_cases_per_10k = state_phase3_cases / (state_pop/10000)
gen state_phase4_cases_per_10k = state_phase4_cases / (state_pop/10000)

gen state_phase1_cases_per_100k = state_phase1_cases_per_10k * 10
gen state_phase2_cases_per_100k = state_phase2_cases_per_10k * 10
gen state_phase3_cases_per_100k = state_phase3_cases_per_10k * 10
gen state_phase4_cases_per_100k = state_phase4_cases_per_10k * 10

egen pre_state_total_cases = total(cases) if date==max_tract_date, by(state_fips)
egen state_total_cases = max(pre_state_total_cases), by(state_fips)
gen state_total_cases_per_10k = state_total_cases / (state_pop/10000)

gen state_total_cases_per_100k = state_total_cases_per_10k * 10

gen pre_tract_total_cases_per_10k = cases_per_10k if date==max_tract_date
egen tract_total_cases_per_10k = max(pre_tract_total_cases_per_10k), by(tract_fips10)

gen tract_total_cases_per_100k = tract_total_cases_per_10k * 10

gen tract_cases = cases 

save "`outfile'", replace

* make subset of one row per tract of positive cases per 10k on last day of data
keep if date==max_tract_date

keep if state_fips == "55"

* make a subset with just analysis vars
keep state_fips tract_fips10 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k tract_cases tract_pop min_date max_date state_total_cases state_pop

order state_fips tract_fips10 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k min_date max_date tract_cases tract_pop min_date max_date state_total_cases state_pop

sort state_fips

save "`outfile_subset'", replace

codebook
summ , format

capture log close


