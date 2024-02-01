local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Virginia"
log using "`script_dir'process_`state_name'_dates_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local out_data_dir "D:/covid/Noppert/data/"
local infile_stub "VirginiaCasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"


local outfile "`out_data_dir'`state_name'CasesByZipCode2022_`version'.dta"
local outfile_date_subset "`out_data_dir'`state_name'_covid_zipcode_date_subset_`version'.dta"
local outfile_subset "`out_data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", asdouble clear 
browse
codebook

* this file is zip / date level with cumulative count, tag last date

*clonevar tract_fips10 = geoid
clonevar zip_code = zipcode

*rename reportdate datetime_str
gen date_str = reportdate

* make date vars
*capture drop date
gen date = date(date_str, "MDY", 2050)
format %td date

egen min_date = min(date), by(zip_code) 
format %td min_date

egen max_date = max(date), by(zip_code) 
format %td max_date

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

*gen cases = confirmed
destring(numberofcases), gen(cases) force
label var cases "Positive COVID-19 cases"

sort zip_code date
egen max_zip_phase_date = max(date), by(zip_code phase) 
format %td max_zip_phase_date

sort zip_code date
gen tag_last_zip_date = .
replace tag_last_zip_date = 1 if date == max_date

*gen cases_per_10k = cases / (population/10000)
*label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "51"

egen max_state_phase_date = max(date), by(state_fips phase) 
format %td max_state_phase_date

* make running sums
sort state_fips zip_code date
egen tag_zip_code = tag(zip_code)
*gen cuml_cases = cases if tag_zip_code == 1
*replace cuml_cases = cuml_cases[_n-1] + cases if state_fips == state_fips[_n-1] & zip_code == zip_code[_n-1]

* make phase sums for state and zip_code
*egen zip_phase_cases = total(cases), by(state_fips zip_code phase)
*egen state_phase_cases = total(cases), by(state_fips phase)

gen pre_zip_phase1_cases = cases if phase==1 & date==max_zip_phase_date 
gen pre_zip_phase2_cases = cases if phase==2 & date==max_zip_phase_date
gen pre_zip_phase3_cases = cases if phase==3 & date==max_zip_phase_date
gen pre_zip_phase4_cases = cases if phase==4 & date==max_zip_phase_date

/*
gen pre_state_phase1_cases = state_phase_cases if phase==1 & date==max_state_phase_date 
gen pre_state_phase2_cases = state_phase_cases if phase==2 & date==max_state_phase_date
gen pre_state_phase3_cases = state_phase_cases if phase==3 & date==max_state_phase_date
gen pre_state_phase4_cases = state_phase_cases if phase==4 & date==max_state_phase_date
*/
*needs work, should be subtraction
egen zip_phase1_cuml_cases = max(pre_zip_phase1_cases), by(zip_code)
egen zip_phase2_cuml_cases = max(pre_zip_phase2_cases), by(zip_code)
egen zip_phase3_cuml_cases = max(pre_zip_phase3_cases), by(zip_code)
egen zip_phase4_cuml_cases = max(pre_zip_phase4_cases), by(zip_code)

gen zip_phase1_cases = zip_phase1_cuml_cases
gen zip_phase2_cases = zip_phase2_cuml_cases - zip_phase1_cuml_cases
gen zip_phase3_cases = zip_phase3_cuml_cases - zip_phase2_cuml_cases
gen zip_phase4_cases = zip_phase4_cuml_cases - zip_phase3_cuml_cases

egen state_phase1_cases = total(zip_phase1_cases), by(state_fips)
egen state_phase2_cases = total(zip_phase2_cases), by(state_fips)
egen state_phase3_cases = total(zip_phase3_cases), by(state_fips)
egen state_phase4_cases = total(zip_phase4_cases), by(state_fips)

egen zip_cases = rowtotal(zip_phase1_cases-zip_phase4_cases)
*egen state_cases = rowtotal(state_phase1_cases-state_phase4_cases)



sort zip_code
save "`outfile'", replace

/*
preserve
* make a zip code level subset for merging
local keepvars "zip_phase1_cases zip_phase2_cases zip_phase3_cases zip_phase4_cases zip_cases state_phase1_cases state_phase2_cases state_phase3_cases state_phase4_cases state_cases"
keep if tag_zip_code == 1
keep state_fips zip_code `keepvars'  

save "`outfile_date_subset'", replace
restore
*/


keep if tag_last_zip_date == 1

egen state_cases = total(cases), by(state_fips)

*keep if state_fips == "17"
keep state_fips zip_code cases state_cases min_date max_date /* cases_per_10k */
order state_fips zip_code cases state_cases min_date max_date  /* cases_per_10k */
sort zip_code

save "`outfile_subset'", replace

codebook 

capture log close
