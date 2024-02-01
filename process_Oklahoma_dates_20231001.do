local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Oklahoma"
log using "`script_dir'process_`state_name'_dates_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "`state_name'CasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local out_data_dir "D:/covid/Noppert/data/Covid-19Data/ZipcodeLevel/"
local outfile "`out_data_dir'`state_name'ZipcodeData_`version'.dta"
local outfile_subset "`out_data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", stringcols(1) clear 
browse
codebook

clonevar zip_code = zip

capture drop date_str
gen date_str = rpt_date

*capture drop date
gen date = date(date_str, "MDY", 2050)
format %td date

egen min_date = min(date), by(zip_code)
format %td min_date
tab1 min_date

egen max_date = max(date), by(zip_code)
format %td max_date
tab1 max_date


rename number number_str

gen type_cases = real(number_str)
replace type_cases = 2 if number_str == "1 to 4"

egen cases = total(type_cases), by(zip_code)

sort zip_code date
egen tag_zip_code = tag(zip_code)
* tag first missing zip code so we can include all cases for state
replace tag_zip_code = 1 if zip == "" & status == "Active"

gen state_fips = "40"

save "`outfile'", replace

keep if tag_zip_code == 1

egen state_cases = total(cases)

keep state_fips zip_code date cases state_cases min_date max_date
order state_fips zip_code date cases state_cases min_date max_date
sort zip_code

save "`outfile_subset'", replace

codebook 
summ, format

capture log close
