local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Florida"
log using "`script_dir'process_`state_name'_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "`state_name'ZipcodeData"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local outfile "`data_dir'`infile_stub'_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", asdouble stringcols(2) clear 
browse
codebook

*this file has more than one row per zip (zip, place level)
*need to sum by zip

*clonevar tract_fips10 = geoid
clonevar zip_code = zip

/*
rename reportdate datetime_str
gen date_str = substr(datetime_str,1,9)

* make date vars
capture drop date
gen date = date(date_str, "MDY", 2050)
format %td date
*/

capture drop date
gen date = date("5/23/2021", "MDY", 2050)
format %td date

egen min_date = min(date), by(zip_code)
format %td min_date

egen max_date = max(date), by(zip_code)
format %td max_date

gen zip_place_cases = labely
replace zip_place_cases = . if labely < 0
bigtab zip_place_cases labely cases_1 
label var zip_place_cases "Positive COVID-19 cases"

*this file has more than one row per zip (zip, place level)
*need to sum by zip
egen cases = total(zip_place_cases), by(zip_code)

egen tag_zip_code = tag(zip_code)

*gen cases_per_10k = cases / (population/10000)
*label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "12"

sort zip_code
save "`outfile'", replace

*keep if state_fips == "17"
keep if tag_zip_code == 1
keep state_fips zip_code cases min_date max_date /* cases_per_10k */
order state_fips zip_code cases min_date max_date /* cases_per_10k */
sort zip_code

save "`outfile_subset'", replace

codebook
summ , format

capture log close
