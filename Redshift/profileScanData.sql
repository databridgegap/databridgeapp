-- 0. How many Scan's on and Scan's Off?
select 'On ' t, count(*) rc, count(distinct TravelDate, CardID, DateTime) sc from stg.ScanOn
union
select 'Off' t, count(*) rc, count(distinct TravelDate, CardID, DateTime) sc from stg.ScanOff
;

-- 1. How many stops are missing scans ?
with all_stops as (
	select distinct (stopID) from stg.ScanOn
	union all
	select distinct (stopID) from stg.ScanOff
), missing_stops as (
	select distinct stopID from all_stops
	minus
	select distinct StopLocationID as StopID from stg.StopLocation
)
select count(*) from missing_stops
; -- 1,350

-- 2. what stop locations have no stoppings

with all_stops as (
	select distinct (stopID) from stg.ScanOn
	union all
	select distinct (stopID) from stg.ScanOff
), no_stoppings as (
	select distinct StopLocationID as StopID from stg.StopLocation
	minus
	select distinct stopID from all_stops
)
select StopLocationID, StopType, StopNameShort, StopNameLong, SuburbName, RegionName
from stg.StopLocation s
join no_stoppings n on s.StopLocationID = n.StopID
; -- country areas mostly


-- 3. are the stg_tt.stops stop_ids the same as stg.StopLocation StopLocationIDs

select	ttstops.stop_id
	,	ttstops.stop_name
	,	stoploc.StopLocationID
	,	stoploc.StopNameShort
	,	stoploc.StopNameLong
	,	stoploc.StopType
	,	stoploc.SuburbName
from stg_tt.stops ttstops
--full outer
join stg.StopLocation stoploc  on ttstops.stop_id = stoploc.StopLocationID
limit 999
; -- stop_name = stopnamelong

-- 4. what tt stops are not in the sample dimension table
with all_stops as (
	select distinct (stopID) from stg.ScanOn
	union all
	select distinct (stopID) from stg.ScanOff
), stops_out_of_sample as (
	select distinct stop_id from stg_tt.stops
		minus
	select distinct stopID as stop_id from all_stops
)
select stop_id, stop_name
from stg_tt.stops s
join stops_out_of_sample o using(stop_id)
; -- lots, mostly contry


-- 5. what sample stoplocations are not in the sample dimension table
with all_stops as (
	select distinct (stopID) from stg.ScanOn
	union all
	select distinct (stopID) from stg.ScanOff
), stops_out_of_sample as (
	select distinct stop_id from stg_tt.stops
		minus
	select distinct stopID as stop_id from all_stops
)
select stop_id, stop_name
from stg_tt.stops s
join stops_out_of_sample o using(stop_id)
; -- lots, mostly contry

-- 6
select 'stg.ScanOn' t, count (distinct stopID) stops, count(*) rc from stg.ScanOn
	union all
select 'stg.ScanOff' t, count (distinct stopID) stops, count(*) rc from stg.ScanOff
	union all
select 'stg_tt.stops' t, count(distinct stop_ID) stops, count(*) rc from stg_tt.stops
	union all 
select 'stg.StopLocation' t, count (distinct StopLocationID) stops, count(*) rc from stg.StopLocation;
;

select count(*)
from model2.flat_joined_scans
; -- 167,603,671

select count(*)
from model2.flat_joined_scans
 where scan_on_stop_name is not null
   and scan_off_stop_name is not null
; -- 66,711,824

select count(*)
from model2.flat_joined_scans
 where scan_on_stop_name is not null
   and scan_off_stop_name is not null
   and scan_on_RouteID != scan_off_RouteID
; -- 19,299,746

