local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Maryland"
log using "`script_dir'process_`state_name'_dates_`version'.log", replace
local state_name "Maryland"
local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "`state_name'CasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local out_data_dir "D:/covid/Noppert/data/"
local outfile "`out_data_dir'`state_name'ZipcodeData_`version'.dta"
local outfile_date_subset "`out_data_dir'`state_name'_covid_zipcode_date_subset_`version'.dta"
local outfile_subset "`out_data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", asdouble stringcols(2) clear 
browse
codebook

*clonevar tract_fips10 = geoid
*clonevar zip_code = postcode

/*
rename reportdate datetime_str
gen date_str = substr(datetime_str,1,9)

* make date vars
capture drop date
gen date = date(date_str, "MDY", 2050)
format %td date
*/
* make date vars
gen min_date = date("4/11/2020", "MDY", 2050)
format %td min_date

gen max_date = date("7/11/2022", "MDY", 2050)
format %td max_date

gen cases = total07_11_2022
label var cases "Positive COVID-19 cases"

gen zip_phase1_cases = total05_31_2020
gen zip_phase2_cases = total08_31_2020 - total05_31_2020
gen zip_phase3_cases = total12_31_2020 - total08_31_2020
gen zip_phase4_cases = total07_11_2022 - total12_31_2020

*gen cases_per_10k = cases / (population/10000)
*label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "24"

egen state_phase1_cases = total(zip_phase1_cases), by(state_fips)
egen state_phase2_cases = total(zip_phase2_cases), by(state_fips)
egen state_phase3_cases = total(zip_phase3_cases), by(state_fips)
egen state_phase4_cases = total(zip_phase4_cases), by(state_fips)

*egen zip_cases = rowtotal(zip_phase1_cases-zip_phase4_cases)
*egen state_cases = rowtotal(state_phase1_cases-state_phase4_cases)
gen zip_cases = cases
egen state_cases = total(zip_cases)

sort zip_code
save "`outfile'", replace

/*
preserve
* make sure its zip_code level
unique zip_code
sort zip_code
keep state_fips zip_code zip_phase1_cases-zip_phase4_cases state_phase1_cases-state_phase4_cases cases zip_cases state_cases
save "`outfile_date_subset'", replace
restore
*/

*keep if state_fips == "17"
keep state_fips zip_code cases state_cases min_date max_date /* cases_per_10k */
order state_fips zip_code cases state_cases min_date max_date /* cases_per_10k */
sort zip_code

save "`outfile_subset'", replace

codebook 

capture log close
