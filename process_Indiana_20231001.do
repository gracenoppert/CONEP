local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "Indiana"
log using "`script_dir'process_`state_name'_`version'.log", replace

local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local infile_stub "IndianaCasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "O:/NaNDA/Data/crosswalks/zip_zcta_2019/datasets/zip_to_zcta_2019.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"

local outfile "`data_dir'`state_name'ZipcodeData_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", asdouble varnames(1) clear 
browse
codebook

*clonevar tract_fips10 = geoid
clonevar zip_code = zip_cd

*rename reportdate datetime_str
*gen date_str = data_snapshot_date

* make date vars
*capture drop date
*gen date = date(date_str, "YMD", 2050)
*format %td date

gen max_date = date("5/19/2022", "MDY", 2050)
format %td max_date

rename population population_str
destring(population_str), gen(population) force
bigtab population population_str 

clonevar zip_pop = population

destring(patient_count), gen(cases) force
bigtab cases patient_count
label var cases "Positive COVID-19 cases"

gen cases_per_10k = cases / (population/10000)
label var cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen state_fips = "18"

egen state_pop = total(zip_pop), by(state_fips)

egen state_cases = total(cases), by(state_fips)

gen state_cases_per_10k = state_cases / (state_pop/10000)
label var state_cases_per_10k "State positive COVID-19 case rate per 10,000 people"

sort zip_code
save "`outfile'", replace

*keep if state_fips == "17"
keep state_fips zip_code cases cases_per_10k zip_pop max_date state_pop state_cases state_cases_per_10k
order state_fips zip_code cases cases_per_10k zip_pop max_date state_pop state_cases state_cases_per_10k
sort zip_code

save "`outfile_subset'", replace

codebook 
summ, format

capture log close