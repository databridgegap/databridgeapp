create schema model2;


drop table if exists model2.joined_scans_to_TravelDate;
create table model2.joined_scans_to_TravelDate  as
select
	 row_number() over ( partition by cast(n.cardid as varchar(10))||'||'||cast(n.DateTime as varchar(20)) order by f.DateTime ) as just1
	,n.TravelDate
	,n.CardID
	,n.DateTime    scan_on_DateTime
	,f.DateTime    scan_off_DateTime
from stg.ScanOn n  -- 213,003,376
join stg.ScanOff f -- 239,291,793
  on n.cardid = f.cardid
 and n.TravelDate = f.TravelDate -- 678,771,951
 and n.DateTime <= f.DateTime    -- 448,621,483
; 

drop table if exists model2.joined_scanons_to_next_scanoff;
create table model2.joined_scanons_to_next_scanoff as 
select distinct * from model2.joined_scans_to_TravelDate
where just1=1
; -- 167,603,656

select count(*)
from model2.joined_scanons_to_next_scanoff
;-- 255,244,346
select top 99 *
from model2.joined_scanons_to_next_scanoff
;


drop table if exists model2.flat_joined_scans2;
create table model2.flat_joined_scans2 distkey(cardid) sortkey(traveldate, cardid) as
select distinct
	 n.CardID
	,n.TravelDate

	,cal.calendaryear
	,cal.monthnumber
	,cal.calendarmonth
	,cal.calendarweek
	,cal.weekdayseq
	,cal.weekday
	,cal.holidays
	,cal.weekending
	,cal.absweek

	,n.CardType              scan_on_CardType
	,con.CardSubTypeDesc     scan_on_CardSubTypeDesc
	,con.PaymentType         scan_on_PaymentType
	,con.FareType            scan_on_FareType
	,con.ConcessionType      scan_on_ConcessionType
	,con.MICardGroup         scan_on_MICardGroup

	,n.DateTime    scan_on_DateTime
	,f.DateTime    scan_off_DateTime

	,n.VehicleID   scan_on_VehicleID
	,f.VehicleID   scan_off_VehicleID

	,n.Mode                   scan_on_Mode
	,n.StopID                 scan_on_StopID
	,slon.StopType            scan_on_StopType
	,slon.StopNameShort       scan_on_StopNameShort
	,slon.StopNameLong        scan_on_StopNameLong
	,slon.SuburbName          scan_on_SuburbName
	,slon.RegionName          scan_on_RegionName
	,slon.GPSLat              scan_on_GPSLat
	,slon.GPSLong             scan_on_GPSLong

	,ttson.stop_name          scan_on_stop_name
	,ttson.stop_lat           scan_on_stop_lat
	,ttson.stop_lon           scan_on_stop_lon

	,f.Mode                   scan_off_Mode
	,f.StopID                 scan_off_StopID
	,slof.StopType            scan_off_StopType
	,slof.StopNameShort       scan_off_StopNameShort
	,slof.StopNameLong        scan_off_StopNameLong
	,slof.SuburbName          scan_off_SuburbName
	,slof.RegionName          scan_off_RegionName
	,slof.GPSLat              scan_off_GPSLat
	,slof.GPSLong             scan_off_GPSLong

	,ttsof.stop_name          scan_off_stop_name
	,ttsof.stop_lat           scan_off_stop_lat
	,ttsof.stop_lon           scan_off_stop_lon

	,f.CardType              scan_off_CardType
	,cof.CardSubTypeDesc     scan_off_CardSubTypeDesc
	,cof.PaymentType         scan_off_PaymentType
	,cof.FareType            scan_off_FareType
	,cof.ConcessionType      scan_off_ConcessionType
	,cof.MICardGroup         scan_off_MICardGroup

	,n.ParentRoute           scan_on_ParentRoute
	,n.RouteID               scan_on_RouteID

	,f.ParentRoute           scan_off_ParentRoute
	,f.RouteID               scan_off_RouteID

from joined_scanons_to_next_scanoff l
join stg.ScanOn  n on n.cardid=l.cardid and n.DateTime=l.scan_on_DateTime
join stg.ScanOff f on f.cardid=l.cardid and f.DateTime=l.scan_off_DateTime
left
join stg.calendar cal on cal.traveldate = n.TravelDate
left
join stg.StopLocation slon on slon.StopLocationId = n.stopid
left 
join stg.StopLocation slof on slof.StopLocationId = f.stopid
left
join stg_tt.stops ttson on ttson.stop_id = n.stopid
left 
join stg_tt.stops ttsof on ttsof.stop_id = f.stopid
left
join stg.CardSubType con on con.CardSubTypeID = n.CardType
left
join stg.CardSubType cof on cof.CardSubTypeID = f.CardType
; 

drop table if exists model2.flat_joined_scans;
alter table model2.flat_joined_scans2 rename to flat_joined_scans;

select count(*)
from model2.flat_joined_scans
;-- 255,244,346
select top 99 *
from model2.flat_joined_scans
;

unload ('select * from model2.flat_joined_scans')   
to 's3://databridge-syd/joined_scans/flat_joined_scans-' 
iam_role '<<>>'
allowoverwrite
parallel on
maxfilesize 800 mb
delimiter '|' gzip;

