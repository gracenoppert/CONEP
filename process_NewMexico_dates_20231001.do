local version "20231001"

local script_dir "D:/covid/Noppert/scripts/"
capture log close
log using "`script_dir'process_NewMexico_date_`version'.log", replace

local in_data_dir "D:/covid/Noppert/acquired/CoNeP/CoNePStateInfo/CoNePStateInfo/NewMexico/"
local data_dir "D:/covid/Noppert/data/Covid-19Data/CensusTractLevel/"
local infile_stub "NewMexicoMonthlyCasesByCensusTract2022"
local infile "`in_data_dir'`infile_stub'.csv"
local outfile "`data_dir'`infile_stub'_date_`version'.dta"
local outfile_subset "`data_dir'`infile_stub'_date_subset_`version'.dta"

cd "`data_dir'"
import delimited "`infile'", stringcols(1) asdouble clear 
browse
codebook

capture drop date_str
gen date_str = inv_start_mo_yr + "-01"

*capture drop date
gen date = date(date_str, "20YMD")
browse
format %td date
browse

gen state_fips = "35"

clonevar tract_fips20 = geoid20

merge m:1 tract_fips20 using "D:/Census/ACS/2020/data/Tracts_Block_Groups_Only/acs_2016_2020_tract_ses_subset.dta", keep(master match) gen(_merge_acs2020)

* positive cases = confirmed cases + probable cases
* should I use cumulative or not?

*positive always (almost always?) > cumulative
egen max_date = max(date), by(tract_fips20)
format %td max_date
tab1 max_date

egen min_date = max(date), by(tract_fips20)
format %td min_date
tab1 min_date

clonevar tract_cases = covid_cases 
egen state_cases = total(covid_cases), by(state_fips date)

gen tract_cases_per_10k = tract_cases / (totpop20/10000)
label var tract_cases_per_10k "Positive COVID-19 case rate per 10,000 people"

gen tract_cases_per_100k = tract_cases_per_10k * 10
label var tract_cases_per_10k "Positive COVID-19 case rate per 100,000 people"

* make phase variable
label define phasef 1 "April-May 2020" 2 "June-August 2020" 3 "September-December 2020" 4 "Beyond December 2020"
gen phase = .
replace phase = 1 if date >= td(01jan2020) & date < td(01jun2020)
replace phase = 2 if date >= td(01jun2020) & date < td(01sep2020)
replace phase = 3 if date >= td(01sep2020) & date < td(01jan2021)
replace phase = 4 if date >= td(01jan2021) & !missing(date)
label values phase phasef
tab1 phase
bigtab phase date

sort tract_fips20 date
egen max_tract_phase_date = max(date), by(tract_fips20 phase)
format %td max_tract_phase_date

egen tract_phase_cases = total(tract_cases), by(tract_fips20 phase)
gen tract_phase_cases_per_10k = tract_phase_cases / (totpop20/10000)

egen state_phase_cases = total(covid_cases), by(state_fips phase)
gen state_phase_cases_per_10k = state_phase_cases / (state_totpop20/10000) 

sort tract_fips20 date
gen tag_last_tract_date = .
replace tag_last_tract_date = 1 if date == max_date

gen pre_tract_phase1_cases_per_10k = tract_phase_cases_per_10k if phase==1
gen pre_tract_phase2_cases_per_10k = tract_phase_cases_per_10k if phase==2
gen pre_tract_phase3_cases_per_10k = tract_phase_cases_per_10k if phase==3
gen pre_tract_phase4_cases_per_10k = tract_phase_cases_per_10k if phase==4

egen tract_phase1_cases_per_10k = max(pre_tract_phase1_cases_per_10k), by(tract_fips20)
egen tract_phase2_cases_per_10k = max(pre_tract_phase2_cases_per_10k), by(tract_fips20)
egen tract_phase3_cases_per_10k = max(pre_tract_phase3_cases_per_10k), by(tract_fips20)
egen tract_phase4_cases_per_10k = max(pre_tract_phase4_cases_per_10k), by(tract_fips20)
replace tract_phase4_cases_per_10k = . if tract_phase4_cases_per_10k >= 10000

gen tract_phase1_cases_per_100k = tract_phase1_cases_per_10k * 10
gen tract_phase2_cases_per_100k = tract_phase2_cases_per_10k * 10
gen tract_phase3_cases_per_100k = tract_phase3_cases_per_10k * 10
gen tract_phase4_cases_per_100k = tract_phase4_cases_per_10k * 10

gen pre_state_phase1_cases_per_10k = state_phase_cases_per_10k if phase==1
gen pre_state_phase2_cases_per_10k = state_phase_cases_per_10k if phase==2
gen pre_state_phase3_cases_per_10k = state_phase_cases_per_10k if phase==3
gen pre_state_phase4_cases_per_10k = state_phase_cases_per_10k if phase==4

egen state_phase1_cases_per_10k = max(pre_state_phase1_cases_per_10k), by(state_fips)
egen state_phase2_cases_per_10k = max(pre_state_phase2_cases_per_10k), by(state_fips)
egen state_phase3_cases_per_10k = max(pre_state_phase3_cases_per_10k), by(state_fips)
egen state_phase4_cases_per_10k = max(pre_state_phase4_cases_per_10k), by(state_fips)

gen state_phase1_cases_per_100k = state_phase1_cases_per_10k * 10
gen state_phase2_cases_per_100k = state_phase2_cases_per_10k * 10
gen state_phase3_cases_per_100k = state_phase3_cases_per_10k * 10
gen state_phase4_cases_per_100k = state_phase4_cases_per_10k * 10

egen tract_total_cases = total(tract_cases), by(tract_fips20)
gen tract_total_cases_per_10k = tract_total_cases / (totpop20/10000)
replace tract_total_cases_per_10k = . if tract_total_cases_per_10k >= 10000
gen tract_total_cases_per_100k = tract_total_cases_per_10k * 10

egen state_total_cases = total(tract_cases), by(state_fips)
gen state_total_cases_per_10k = state_total_cases / (state_totpop20/10000)
gen state_total_cases_per_100k = state_total_cases_per_10k * 10

clonevar tract_pop = totpop20
clonevar state_pop = state_totpop20


save "`outfile'", replace

* make subset of one row per tract of positive cases per 10k on last day of data
unique tract_fips20
keep if tag_last_tract_date == 1

* make a subset with just analysis vars
keep state_fips tract_fips20 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k min_date max_date tract_total_cases state_total_cases tract_pop state_pop

order state_fips tract_fips20 tract_total_cases_per_10k tract_total_cases_per_100k state_total_cases_per_10k state_total_cases_per_100k tract_phase1_cases_per_10k-tract_phase4_cases_per_10k tract_phase1_cases_per_100k-tract_phase4_cases_per_100k state_phase1_cases_per_10k-state_phase4_cases_per_10k state_phase1_cases_per_100k-state_phase4_cases_per_100k min_date max_date tract_total_cases state_total_cases tract_pop state_pop

sort tract_fips20

save "`outfile_subset'", replace

codebook
summ, format

capture log close

