local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Oregon"
log using "`script_dir'process_`state_name'_dates_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "`state_name'CasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local outfile "`data_dir'`state_name'ZipcodeData_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", varnames(2) clear
browse
codebook

clonevar zip_code = zipcode
unique zip_code

* make date vars
capture drop date
gen date = date("6/3/2022", "MDY", 2050)
format %td date

egen min_date = min(date), by(zip_code)
format %td min_date

egen max_date = max(date), by(zip_code)
format %td max_date

destring(casecount), gen(cases) force
label var cases "Positive COVID-19 cases"

destring(populationcount), gen(zip_pop) force

gen cases_per_10k = cases / (zip_pop/10000)
label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "41"

egen state_pop = total(zip_pop), by(state_fips) 

egen state_cases = total(cases), by(state_fips)

gen state_cases_per_10k = state_cases / (state_pop/10000)
label var state_cases_per_10k "State positive COVID-19 case rate per 10,000 people"


sort zip_code
save "`outfile'", replace

*keep if state_fips == "17"
keep state_fips zip_code cases cases_per_10k zip_pop min_date max_date state_pop state_cases state_cases_per_10k
order state_fips zip_code cases cases_per_10k zip_pop min_date max_date state_pop state_cases state_cases_per_10k
sort zip_code

save "`outfile_subset'", replace

codebook 
summ, format
capture log close