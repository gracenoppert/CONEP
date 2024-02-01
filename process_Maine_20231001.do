local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Maine"
log using "`script_dir'process_`state_name'_20231001.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "MaineCasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local outfile "`data_dir'`state_name'ZipcodeData_20231001.dta"
local outfile_subset "`data_dir'`state_name'_covid_zipcode_subset_20231001.dta"

cd "`data_dir'"
import delimited "`infile'", asdouble stringcols(1) clear 
browse
codebook

*clonevar tract_fips10 = geoid
*clonevar zip_code = zip

*rename reportdate datetime_str
gen date_str = data_snapshot_date

* make date vars
*capture drop date
gen date = date(date_str, "YMD", 2050)
format %td date
summ date, format

egen min_date = min(date), by(zip_code)
format %td min_date

egen max_date = max(date), by(zip_code)
format %td max_date

clonevar zip_pop = zip_population

tab1 case_count
destring(case_count), gen(cases) force
replace cases = 0 if case_count == "No Detected Cases"
replace cases = 3 if case_count == "Range of 1-5"
* make integer for Poisson
replace cases = 13 if case_count == "Range of 6-19" 
replace cases = 35 if case_count == "Range of 20-49"
replace cases = 75 if case_count == "Range of 50-99"
replace cases = 175 if case_count == "Range of >100" 
replace cases = 625 if case_count == "Range of >250" 
replace cases = 1000 if case_count == "Range of >1000" 
bigtab cases case_count
label var cases "Positive COVID-19 cases"

gen cases_per_10k = cases / (zip_population/10000)
label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "23"

egen state_pop = total(zip_pop), by(state_fips) 

egen state_cases = total(cases), by(state_fips)

gen state_cases_per_10k = state_cases / (state_pop/10000)
label var state_cases_per_10k "State positive COVID-19 case rate per 10,000 people"

sort zip_code
save "`outfile'", replace

*keep if state_fips == "23"
keep state_fips zip_code cases cases_per_10k zip_pop min_date max_date state_pop state_cases state_cases_per_10k
order state_fips zip_code cases cases_per_10k zip_pop min_date max_date state_pop state_cases state_cases_per_10k
sort zip_code

save "`outfile_subset'", replace

codebook
summ, format

capture log close