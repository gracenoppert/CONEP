local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'process_RhodeIsland_dates_`version'.log", replace

local state_name "RhodeIsland"
local in_data_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/RhodeIsland/" 
local data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
local infile_stub "COVID19-Historical-V2-TRCT"
local infile "`in_data_dir'`infile_stub'.csv"
local outfile "`data_dir'RhodeIslandCensusTract_dates_`version'.dta"
local outfile_subset "`data_dir'`state_name'_covid_tract_date_subset_`version'.dta"

cd "`data_dir'"

import delimited "D:\covid\Noppert\acquired\CoNeP\CoNePStateInfo\CoNePStateInfo\RhodeIsland\RhodeIslandMonthlyCasesByCensusTract2022(2010Census)_rev.csv", numericcols(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23) clear

clonevar tract_fips10 = censustract
gen state_fips = substr(tract_fips10, 1, 2)

* merge in prior months from previous file
merge 1:1 tract_fips10 using "D:\covid\Noppert\data\Covid-19Data\CensusTractLevel\RhodeIslandCasesByCensusTractMonthly_rev.dta", keep(match master) keepusing(month_2020_3 month_2020_4 month_2020_5 month_2020_6 month_2020_7 month_2020_8 month_2020_9)

egen tract_phase1_cases = rowtotal(month_2020_3 month_2020_4 month_2020_5)
egen tract_phase2_cases = rowtotal(month_2020_6 month_2020_7 month_2020_8)
egen tract_phase3_cases = rowtotal(month_2020_9 month_2020_10 month_2020_11 month_2020_12)
egen tract_phase4_cases = rowtotal(month_2021_1 month_2021_2 month_2021_3 month_2021_4 month_2021_5 month_2021_6 month_2021_7 month_2021_8 month_2021_9 month_2021_10 month_2021_11 month_2021_12 month_2022_1 month_2022_2 month_2022_3 month_2022_4 month_2022_5 month_2022_6 month_2022_7)

egen tract_total_cases = rowtotal(tract_phase1_cases tract_phase2_cases tract_phase3_cases tract_phase4_cases)

drop if substr(tract_fips10, 1, 2) != "44"

gen min_date = date("3/1/2020", "MDY", 2050)
format %td min_date
summ min_date, format

gen max_date = date("7/31/2022", "MDY", 2050)
format %td max_date
summ max_date, format

egen state_phase1_cases = total(tract_phase1_cases)
egen state_phase2_cases = total(tract_phase2_cases)
egen state_phase3_cases = total(tract_phase3_cases)
egen state_phase4_cases = total(tract_phase4_cases)

egen state_total_cases = total(tract_total_cases)

merge m:1 tract_fips10 using "D:\Census\ACS\2017\2013-2017\data\Tracts_Block_Groups_Only\nanda_ses_tract_2008-2017_04.dta", keep(master match) keepusing(totpop13_17 state_totpop13_17) gen(_merge_acs2017)

gen tract_phase1_cases_per_10k = tract_phase1_cases / (totpop13_17/10000)
gen tract_phase2_cases_per_10k = tract_phase2_cases / (totpop13_17/10000)
gen tract_phase3_cases_per_10k = tract_phase3_cases / (totpop13_17/10000)
gen tract_phase4_cases_per_10k = tract_phase4_cases / (totpop13_17/10000)

gen tract_total_cases_per_10k = tract_total_cases / (totpop13_17/10000)

gen state_phase1_cases_per_10k = state_phase1_cases / (state_totpop13_17/10000)
gen state_phase2_cases_per_10k = state_phase2_cases / (state_totpop13_17/10000)
gen state_phase3_cases_per_10k = state_phase3_cases / (state_totpop13_17/10000)
gen state_phase4_cases_per_10k = state_phase4_cases / (state_totpop13_17/10000)

gen state_total_cases_per_10k = state_total_cases / (state_totpop13_17/10000)

save "`outfile'", replace

* make subset of one row per tract of positive cases per 10k on last day of data
keep if state_fips == "44"

* make a subset with just analysis vars
keep state_fips tract_fips10 tract_total_cases tract_total_cases_per_10k  state_total_cases_per_10k  tract_phase1_cases_per_10k-tract_phase4_cases_per_10k  state_phase1_cases_per_10k-state_phase4_cases_per_10k min_date max_date

order state_fips tract_fips10 tract_total_cases tract_total_cases_per_10k state_total_cases_per_10k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k  state_phase1_cases_per_10k-state_phase4_cases_per_10k min_date max_date

sort state_fips

save "`outfile_subset'", replace

codebook
summ , format

capture log close


