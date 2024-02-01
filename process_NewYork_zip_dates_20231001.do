local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
local state_name "NewYork"
log using "`script_dir'process_`state_name'_zip_dates_`version'.log", replace
local state_name "NewYork"
local data_base_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/"
local data_dir "`data_base_dir'`state_name'/"
local out_data_dir "D:/covid/Noppert/data/"
local infile_stub "`state_name'DailyCasesByZipCode2022"
local infile "`data_dir'`infile_stub'.csv"
local zip_zcta_xwalk "D:/Census/zip_zcta_xwalk/zip_to_zcta_2019_NY.dta"
local zctacensusfile "O:/NaNDA/Data/ses_demographics/sesdem_zcta_2008-2017/datasets/nanda_ses_zcta_2008-2017_02P"
local zipdotcomfile "D:/Census/zip_codes/zip-codes.com/zip-codes.com_ny_uniq.dta"

local outfile "`out_data_dir'`state_name'CasesByZipCode2022_`version'.dta"
local outfile_date_subset "`out_data_dir'`state_name'_covid_zipcode_date_subset_`version'.dta"
local outfile_subset "`out_data_dir'`state_name'_covid_zipcode_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", asdouble clear 
browse
codebook

* zip, county, date, daily counts (not cumulative)
clonevar zip_code = zipcode
replace zip_code = regexr(zip_code, "[oO]", "0")

gen good_zip = (regexm(zip_code, "^[ \t]*[0-9][0-9][0-9][0-9][0-9][ \t]*$"))
bigtab good_zip zip_code

gen ny_zip = (regexm(zip_code, "^[ \t]*1"))
bigtab good_zip ny_zip zip_code

unique zip_code if good_zip==1 & ny_zip==1
*Number of unique values of zip_code is  7845
*Number of records is  1956014
* too many, https://www.zip-codes.com/state/ny.asp has 2150
* zip_zcta_crosswalk has 2148

* merge in zip_codes.com list to compare
rename zipcode orig_zipcode
clonevar zipcode = zip_code
merge m:1 zipcode using "`zipdotcomfile'", keep(match master) gen(_merge_zipdotcomfile)

* merge in zip, zcta crosswalk to compare
merge m:1 zip_code using "`zip_zcta_xwalk'", keep(match master) gen(_merge_zipzctaxwalk)

* only two zips on zip-codes.com not in crosswalk, both are zero population
* so we can just keep the zips that merge into crosswalk
bigtab _merge_zipdotcomfile _merge_zipzctaxwalk
tab1 zip_code if _merge_zipdotcomfile != _merge_zipzctaxwalk
bigtab zip_code type if _merge_zipdotcomfile != _merge_zipzctaxwalk

*get date
gen date_str = testdate

* make date vars
*capture drop date
gen date = date(date_str, "MDY", 2050)
format %td date

* get cases
clonevar cases = positivecases

* make phase variable
label define phasef 1 "April-May 2020" 2 "June-August 2020" 3 "September-December 2020" 4 "December 2020-April 2022"
gen phase = .
replace phase = 1 if date >= td(01jan2020) & date < td(01jun2020)
replace phase = 2 if date >= td(01jun2020) & date < td(01sep2020)
replace phase = 3 if date >= td(01sep2020) & date < td(01jan2021)
replace phase = 4 if date >= td(01jan2021) & date < td(01may2022)
label values phase phasef
tab1 phase
bigtab phase date

* get max date by phase and zip
sort zip_code date
egen max_zip_phase_date = max(date), by(zip_code phase) 
format %td max_zip_phase_date

* get max date by zip
sort zip_code date
egen max_zip_date = max(date), by(zip_code) 
format %td max_zip_date

* create date range variables
egen min_date = min(date), by(zip_code) 
format %td min_date

egen max_date = max(date), by(zip_code) 
format %td max_date

* tag the last row for each zip and phase
sort zip_code date
gen tag_last_zip_phase_date = .
replace tag_last_zip_phase_date = 1 if date == max_zip_phase_date

gen state_fips = "36" if _merge_zipzctaxwalk==3 

* create zip phase case totals
egen _zip_phase1_cases = total(cases) if phase==1, by(zip_code)
egen _zip_phase2_cases = total(cases) if phase==2, by(zip_code)
egen _zip_phase3_cases = total(cases) if phase==3, by(zip_code)
egen _zip_phase4_cases = total(cases) if phase==4, by(zip_code)

egen zip_phase1_cases = max(_zip_phase1_cases), by(zip_code)
egen zip_phase2_cases = max(_zip_phase2_cases), by(zip_code)
egen zip_phase3_cases = max(_zip_phase3_cases), by(zip_code)
egen zip_phase4_cases = max(_zip_phase4_cases), by(zip_code)

egen _state_phase1_cases = total(cases) if phase==1 & _merge_zipzctaxwalk==3, by(state_fips)
egen _state_phase2_cases = total(cases) if phase==2 & _merge_zipzctaxwalk==3, by(state_fips)
egen _state_phase3_cases = total(cases) if phase==3 & _merge_zipzctaxwalk==3, by(state_fips)
egen _state_phase4_cases = total(cases) if phase==4 & _merge_zipzctaxwalk==3, by(state_fips)

egen state_phase1_cases = max(_state_phase1_cases), by(state_fips)
egen state_phase2_cases = max(_state_phase2_cases), by(state_fips)
egen state_phase3_cases = max(_state_phase3_cases), by(state_fips)
egen state_phase4_cases = max(_state_phase4_cases), by(state_fips)

egen zip_phase_cases = rowtotal(zip_phase1_cases-zip_phase4_cases)
egen state_phase_cases = rowtotal(state_phase1_cases-state_phase4_cases)

* create zip total cases
egen zip_cases = total(cases), by(zip_code)

* create state total cases
egen state_cases = total(cases) if _merge_zipzctaxwalk==3, by(state_fips)

sort zip_code date
egen tag_zip_code = tag(zip_code)

save "`outfile'", replace

preserve
* make a zip code level subset for merging
local keepvars "zip_phase1_cases zip_phase2_cases zip_phase3_cases zip_phase4_cases zip_cases state_phase1_cases state_phase2_cases state_phase3_cases state_phase4_cases state_cases min_date max_date"

* keep only one record per zip code
keep if tag_zip_code==1 & _merge_zipzctaxwalk==3
unique zip_code
keep state_fips zip_code `keepvars'  
order state_fips zip_code `keepvars'
save "`outfile_date_subset'", replace
restore

summ date, format

capture log close

