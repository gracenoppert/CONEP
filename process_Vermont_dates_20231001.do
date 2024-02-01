local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Vermont"
log using "`script_dir'process_`state_name'_dates_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local out_data_dir "D:/covid/Noppert/data/"
local infile_stub "`state_name'WeeklyCasesByZipCode2022"
local infile "`data_dir'`infile_stub'.xlsx"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local outfile "`out_data_dir'`state_name'ZipcodeData_`version'.dta"
local outfile_date_subset "`out_data_dir'`state_name'_covid_zipcode_date_subset_`version'.dta"
local outfile_subset "`out_data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

import excel "`infile'", sheet("Cases_by_Zip") firstrow clear

gen date = case_rpt_week_end
format date %td

egen min_date = min(date), by(zip_code)
format %td min_date
tab1 min_date

egen max_date = max(date), by(zip_code)
format %td max_date
tab1 max_date

* make phase variable
label define phasef 1 "April-May 2020" 2 "June-August 2020" 3 "September-December 2020" 4 "Beyond December 2020"
gen phase = .
replace phase = 1 if date >= td(01apr2020) & date < td(01jun2020)
replace phase = 2 if date >= td(01jun2020) & date < td(01sep2020)
replace phase = 3 if date >= td(01sep2020) & date < td(01jan2021)
replace phase = 4 if date >= td(01jan2021) & date < td(01may2022)
label values phase phasef
tab1 phase
bigtab phase date

gen state_fips = "50"

rename zip_code orig_zip_code
destring(orig_zip_code), gen(zip_num) force 
gen zip_code = string(zip_num,"%05.0f")

destring(total_case_count), gen(cases) force

* make running sums
sort state_fips zip_code date
egen tag_zip_code = tag(zip_code)
gen cuml_cases = cases if tag_zip_code == 1
replace cuml_cases = cuml_cases[_n-1] + cases if state_fips == state_fips[_n-1] & zip_code == zip_code[_n-1]

* make phase sums for state and zip_code
egen zip_phase_cases = total(cases), by(state_fips zip_code phase)
egen state_phase_cases = total(cases), by(state_fips phase)

gen pre_zip_phase1_cases = zip_phase_cases if phase==1
gen pre_zip_phase2_cases = zip_phase_cases if phase==2
gen pre_zip_phase3_cases = zip_phase_cases if phase==3
gen pre_zip_phase4_cases = zip_phase_cases if phase==4

gen pre_state_phase1_cases = state_phase_cases if phase==1
gen pre_state_phase2_cases = state_phase_cases if phase==2
gen pre_state_phase3_cases = state_phase_cases if phase==3
gen pre_state_phase4_cases = state_phase_cases if phase==4

egen zip_phase1_cases = max(pre_zip_phase1_cases), by(zip_code)
egen zip_phase2_cases = max(pre_zip_phase2_cases), by(zip_code)
egen zip_phase3_cases = max(pre_zip_phase3_cases), by(zip_code)
egen zip_phase4_cases = max(pre_zip_phase4_cases), by(zip_code)

egen state_phase1_cases = max(pre_state_phase1_cases), by(state_fips)
egen state_phase2_cases = max(pre_state_phase2_cases), by(state_fips)
egen state_phase3_cases = max(pre_state_phase3_cases), by(state_fips)
egen state_phase4_cases = max(pre_state_phase4_cases), by(state_fips)

*egen zip_cases = rowtotal(zip_phase1_cases-zip_phase4_cases)
*egen state_cases = rowtotal(state_phase1_cases-state_phase4_cases)
egen zip_cases = total(cases), by(state_fips zip_code)
egen state_cases = total(cases), by(state_fips)

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

*keep if state_fips == "17"
keep if tag_zip_code == 1
*keep if cases != .
keep state_fips zip_code zip_cases state_cases min_date max_date /* cases_per_10k */
order state_fips zip_code zip_cases state_cases min_date max_date /* cases_per_10k */
sort zip_code

save "`outfile_subset'", replace

codebook 

capture log close
