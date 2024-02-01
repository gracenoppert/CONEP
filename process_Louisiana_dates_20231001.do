local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'process_Louisiana_date_`version'.log", replace

local in_data_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/Louisiana/"
local data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
local infile_stub "LousianaWeeklyCasesByCensusTract2022"
local infile "`in_data_dir'`infile_stub'.xlsx"
local outfile "`data_dir'`state_name'ZipcodeData_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_zipcode_subset_`version'"

cd "`data_dir'"
import excel "`infile'", sheet("TESTING") firstrow case(lower) clear
browse
codebook

clonevar tract_fips10 = tract

merge m:1 tract_fips10 using "D:\Census\ACS\2017\2013-2017\data\Tracts_Block_Groups_Only\nanda_ses_tract_2008-2017_04.dta", keep(master match) gen(_merge_acs2017)

* make date vars
*gen start_date = date(dateforstartofweek, "MDY", 2050)
clonevar start_date = dateforstartofweek
format %td start_date

*gen end_date = date(dateforendofweek, "MDY", 2050)
clonevar end_date = dateforendofweek
format %td end_date


egen min_date = min(start_date), by(tract_fips10)
format %td min_date
tab1 min_date

egen max_date = max(end_date), by(tract_fips10)
format %td max_date
tab1 max_date

gen tract_cases = weeklycasecount

* make tract level totals
local count_vars "weeklytestcount weeklynegativetestcount weeklypositivetestcount weeklycasecount"
foreach count_var of local count_vars {
	egen tot_`count_var' = total(`count_var'), by(tract_fips10)
}

*gen tract_total_cases = tot_weeklycasecount
*label var tract_total_cases "Positive COVID-19 cases"
*gen state_fips = substr(tract_fips10, 1, 2)

sort tract_fips10 start_date end_date
egen tag_tract_fips10 = tag(tract_fips10)

* make phase variable
label define phasef 1 "April-May 2020" 2 "June-August 2020" 3 "September-December 2020" 4 "Beyond December 2020"
gen phase = .
replace phase = 1 if start_date >= td(01jan2020) & end_date < td(01jun2020)
replace phase = 2 if start_date >= td(01jun2020) & end_date < td(01sep2020)
replace phase = 3 if start_date >= td(01sep2020) & end_date < td(01jan2021)
replace phase = 4 if start_date >= td(01jan2021) & !missing(start_date)
replace phase = 2 if start_date == td(28may2020) & end_date == td(03jun2020)
replace phase = 3 if start_date == td(27aug2020) & end_date == td(02sep2020)
replace phase = 4 if start_date == td(31dec2020) & end_date == td(06jan2021)
label values phase phasef
tab1 phase
bigtab phase start_date end_date


sort tract_fips10 start_date end_date
egen max_tract_phase_date = max(end_date), by(tract_fips10 phase)
format %td max_tract_phase_date

egen tract_phase_cases = total(tract_cases), by(tract_fips10 phase)
gen tract_phase_cases_per_10k = tract_phase_cases / (totpop13_17/10000)

egen state_phase_cases = total(tract_cases), by(state_fips phase)
gen state_phase_cases_per_10k = state_phase_cases / (state_totpop13_17/10000) 

sort tract_fips10 start_date end_date
gen tag_last_tract_date = .
replace tag_last_tract_date = 1 if end_date == max_date

gen pre_tract_phase1_cases_per_10k = tract_phase_cases_per_10k if phase==1
gen pre_tract_phase2_cases_per_10k = tract_phase_cases_per_10k if phase==2
gen pre_tract_phase3_cases_per_10k = tract_phase_cases_per_10k if phase==3
gen pre_tract_phase4_cases_per_10k = tract_phase_cases_per_10k if phase==4

egen tract_phase1_cases_per_10k = max(pre_tract_phase1_cases_per_10k), by(tract_fips10)
egen tract_phase2_cases_per_10k = max(pre_tract_phase2_cases_per_10k), by(tract_fips10)
egen tract_phase3_cases_per_10k = max(pre_tract_phase3_cases_per_10k), by(tract_fips10)
egen tract_phase4_cases_per_10k = max(pre_tract_phase4_cases_per_10k), by(tract_fips10)
replace tract_phase4_cases_per_10k = . if tract_phase4_cases_per_10k >= 10000

gen tract_phase1_cases_per_100k = tract_phase1_cases_per_10k * 10
gen tract_phase2_cases_per_100k = tract_phase2_cases_per_10k * 10
gen tract_phase3_cases_per_100k = tract_phase3_cases_per_10k * 10
gen tract_phase4_cases_per_100k = tract_phase4_cases_per_10k * 10

gen pre_state_phase1_cases_per_10k = state_phase_cases_per_10k if phase==1
gen pre_state_phase2_cases_per_10k = state_phase_cases_per_10k if phase==2
gen pre_state_phase3_cases_per_10k = state_phase_cases_per_10k if phase==3
gen pre_state_phase4_cases_per_10k = state_phase_cases_per_10k if phase==4

egen state_phase1_cases_per_10k = max(pre_state_phase1_cases_per_10k), by(state_fips)
egen state_phase2_cases_per_10k = max(pre_state_phase2_cases_per_10k), by(state_fips)
egen state_phase3_cases_per_10k = max(pre_state_phase3_cases_per_10k), by(state_fips)
egen state_phase4_cases_per_10k = max(pre_state_phase4_cases_per_10k), by(state_fips)

gen state_phase1_cases_per_100k = state_phase1_cases_per_10k * 10
gen state_phase2_cases_per_100k = state_phase2_cases_per_10k * 10
gen state_phase3_cases_per_100k = state_phase3_cases_per_10k * 10
gen state_phase4_cases_per_100k = state_phase4_cases_per_10k * 10

egen tract_total_cases = total(tract_cases), by(tract_fips10)
gen tract_total_cases_per_10k = tract_total_cases / (totpop13_17/10000)
replace tract_total_cases_per_10k = . if tract_total_cases_per_10k >= 10000
gen tract_total_cases_per_100k = tract_total_cases_per_10k * 10

egen state_total_cases = total(tract_cases), by(state_fips)
gen state_total_cases_per_10k = state_total_cases / (state_totpop13_17/10000)
gen state_total_cases_per_100k = state_total_cases_per_10k * 10

clonevar tract_pop = totpop13_17
clonevar state_pop = state_totpop13_17

save "`outfile'", replace

* make subset of one row per tract of positive cases per 10k on last day of data
unique tract_fips10
keep if tag_last_tract_date == 1

* make a subset with just analysis vars
keep state_fips tract_fips10 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k tract_total_cases tract_pop state_total_cases state_pop min_date max_date

order state_fips tract_fips10 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k tract_total_cases tract_pop state_total_cases state_pop min_date max_date

sort tract_fips10

save "`outfile_subset'", replace

codebook
summ, format

capture log close

