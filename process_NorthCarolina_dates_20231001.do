local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "NorthCarolina"
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
import delimited "`infile'", stringcols(1) clear
browse
codebook

clonevar zip_code = zipcode

rename cases cases_str
rename casesper10000residents casesper10000residents_str
rename casesper100000residents casesper100000residents_str

destring(cases_str), gen(cases) ignore(",")
destring(casesper10000residents_str), gen(casesper10000residents) ignore(",")
destring(casesper100000residents_str), gen(casesper100000residents) ignore(",")

* make date vars
capture drop date
gen date = date("5/26/2022", "MDY", 2050)
format %td date

egen min_date = min(date), by(zip_code)
format %td min_date

egen max_date = max(date), by(zip_code)
format %td max_date


*destring(case_count), gen(cases) force
label var cases "Positive COVID-19 cases"

*destring(population_count), gen(population) force

gen cases_per_10k = casesper10000residents
label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen cases_per_100k = casesper100000residents
label var cases_per_10k "Positive COVID-19 case rate per 100,000 people"

gen long zip_pop = round(((cases/cases_per_10k)*10000),1)
label var zip_pop "Zip code population"

gen state_fips = "37"

egen state_pop = total(zip_pop), by(state_fips) 

egen state_cases = total(cases), by(state_fips)

gen state_cases_per_10k = state_cases / (state_pop/10000)
label var state_cases_per_10k "State positive COVID-19 case rate per 10,000 people"

sort zip_code
save "`outfile'", replace

*keep if state_fips == "37"
keep state_fips zip_code cases cases_per_10k zip_pop min_date max_date state_pop state_cases state_cases_per_10k
order state_fips zip_code cases cases_per_10k zip_pop min_date max_date state_pop state_cases state_cases_per_10k
sort zip_code

save "`outfile_subset'", replace

codebook 

capture log close