local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Illinois"
log using "`script_dir'process_`state_name'_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "IllinoisCasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local outfile "`data_dir'`infile_stub'_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", stringcols(1) clear
browse
codebook

*clonevar tract_fips10 = geoid
clonevar zip_code = zip

*rename reportdate datetime_str
*gen date_str = substr(datetime_str,1,9)

* make date vars
*capture drop date
gen date = date("7/12/2022", "MDY", 2050)
format %td date

egen min_date = min(date), by(zip_code)
format %td min_date

egen max_date = max(date), by(zip_code)
format %td max_date

capture drop cases
destring(totalcases), gen(cases) ignore(",")
*gen cases = real(totalcases)
label var cases "Positive COVID-19 cases"

*gen cases_per_10k = positive / (pop/10000)
*label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "17"

egen state_cases = total(cases), by(state_fips)

sort zip_code date
save "`outfile'", replace

*keep if state_fips == "17"
keep state_fips zip_code cases state_cases min_date max_date
order state_fips zip_code cases state_cases min_date max_date
sort zip_code

save "`outfile_subset'", replace

codebook 

capture log close
