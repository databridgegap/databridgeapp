-- How many Scan's on Sep 1, 2017
select count(*) rc, count(distinct cardid) cards
from stg.ScanOn n  -- 1,078,686,595
where n.TravelDate = '2017-09-01' --  1,254,221 - 558,358 
;

-- How unique are the scans-on by CardID and DateTime
with uniques as (
	select CardID, s.DateTime dt, count(*) qty
	from stg.ScanOn s
	group by 1,2
)
select qty, count(*) qtyofqty
from uniques 
group by 1
order by 1 desc
;
-- 2	14
-- 1	1078686567
;

-- How unique are the scans-of by CardID and DateTime
with uniques as (
	select CardID, s.DateTime dt, count(*) qty
	from stg.ScanOff s
	group by 1,2
)
select qty, count(*) qtyofqty
from uniques 
group by 1
order by 1 desc
;
-- 2	125025
-- 1	802757109

-- How many scans-on join with scans-Off?
select count(*) rc 
from stg.ScanOn n  -- 1,078,686,595
left
join stg.ScanOff f --   803,007,159
  on n.cardid = f.cardid
 and n.TravelDate = f.TravelDate -- 2,300,806,007
 and n.DateTime <= f.DateTime    -- 1,521,217,401
 and n.TravelDate = '2017-09-01' --     1,765,667
 ;

-- How many scans-on join with scans-Off (include no scan-off)?
select count(*) rc 
from stg.ScanOn n  -- 1,078,686,595
left
join stg.ScanOff f --   803,007,159
  on n.cardid = f.cardid
 and n.TravelDate = f.TravelDate  -- 2,439,519,884
 and n.DateTime <= f.DateTime     -- 1,733,747,457
where n.TravelDate = '2017-09-01' --     2,012,199
  and f.TravelDate = '2017-09-01' --     1,765,667
 ;

create table stg.uScanOn distkey(CardID) sortkey(TravelDate, CardID, DateTime) as
	select distinct s.*
	from stg.ScanOn s
	order by TravelDate, CardID, DateTime
;

create table stg.uScanOff distkey(CardID) sortkey(TravelDate, CardID, DateTime) as
	select distinct s.*
	from stg.ScanOff s
	order by TravelDate, CardID, DateTime
;

vacuum stg.uScanOn to 100 percent;
vacuum stg.uScanOff to 100 percent;
set analyze_threshold_percent to 0;
analyze;


-- match every scan on with every scan off for a given card on a given travel day
-- so if there are say, two scans-on and two scans-off, then each on-scan will be match with each off-scan
-- thus resulting in four output rows.
-- we will only want to include the first scan-off for each scan-on, and the last scan-on for each scan-off,
-- thus resulting in two rows in our example.
drop table if exists model2.joined_card_scans_on_TravelDate;
create table model2.joined_card_scans_on_TravelDate distkey(CardID) sortkey(TravelDate, cardid, scan_on_DateTime) as 
with join_all as (
	-- match every scan on with every scan off for a given card on a given travel day
	-- so if there are say, two scans-on and two scans-off, then each on-scan will be match with each off-scan
	-- thus resulting in four output rows.
	select
	     -- identify a single scan-on by the unique combination of cardid and scan datetime
		 row_number() over ( partition by n.TravelDate, n.CardID, scan_on_DateTime order by f.DateTime ) as first_scan_off
		,n.TravelDate
		,n.CardID
		,n.DateTime    scan_on_DateTime
		,f.DateTime    scan_off_DateTime
	from stg.uScanOn n  -- 1,078,686,595
	left
	join stg.uScanOff f --   803,007,159
	  on n.cardid = f.cardid
	 and n.TravelDate = f.TravelDate -- 2,300,806,007
	 and n.DateTime <= f.DateTime    -- 1,521,217,401
--	 and n.TravelDate = '2017-09-01' --     1,765,667 (for 1 Sep 2017)
), filter_first_scan_off as (
	-- filter in only the first_scan_off for each scan-on, and work out the last scan-on for each scan-off
	select 
	     -- identify a single scan-off by the unique combination of cardid and scan datetime
		 row_number() over ( partition by cardid, scan_off_DateTime order by scan_on_DateTime desc ) as last_scan_on
		,TravelDate, CardID, scan_on_DateTime, scan_off_DateTime
	from join_all
	where first_scan_off=1 -- 1,007,689 (for 1 Sep 2017)
), filter_last_scan_on as (
	-- filter in only the first_scan_off for each scan-on, and work out the last scan-on for each scan-off
	select TravelDate, CardID, scan_on_DateTime, scan_off_DateTime
	from filter_first_scan_off
	where last_scan_on=1 -- 912,642 (for 1 Sep 2017)
)
select * from filter_last_scan_on
order by TravelDate, cardid, scan_on_DateTime, scan_off_DateTime
; 

vacuum model2.joined_card_scans_on_TravelDate to 100 percent;
set analyze_threshold_percent to 0;
analyze;



drop table if exists model2.flat_joined_scans2;
create table model2.flat_joined_scans2 distkey(cardid) sortkey(traveldate, cardid, scan_on_DateTime) as
select
	 l.CardID
	,l.TravelDate

	,cal.calendaryear
	,cal.monthnumber
	,cal.calendarmonth
	,cal.calendarweek
	,cal.weekdayseq
	,cal.weekday
	,cal.holidays
	,cal.weekending
	,cal.absweek

	,n.CardType              CardType
	,con.CardSubTypeDesc     CardSubTypeDesc
	,con.PaymentType         PaymentType
	,con.FareType            FareType
	,con.ConcessionType      ConcessionType
	,con.MICardGroup         MICardGroup

	,l.scan_on_DateTime
	,l.scan_off_DateTime

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

	,f.Mode                   scan_off_Mode
	,f.StopID                 scan_off_StopID
	,slof.StopType            scan_off_StopType
	,slof.StopNameShort       scan_off_StopNameShort
	,slof.StopNameLong        scan_off_StopNameLong
	,slof.SuburbName          scan_off_SuburbName
	,slof.RegionName          scan_off_RegionName
	,slof.GPSLat              scan_off_GPSLat
	,slof.GPSLong             scan_off_GPSLong

	,n.ParentRoute           scan_on_ParentRoute
	,n.RouteID               scan_on_RouteID

	,f.ParentRoute           scan_off_ParentRoute
	,f.RouteID               scan_off_RouteID

from model2.joined_card_scans_on_TravelDate l
join stg.uScanOn  n on n.TravelDate=l.TravelDate and n.cardid=l.cardid and n.DateTime=l.scan_on_DateTime
join stg.uScanOff f on f.TravelDate=l.TravelDate and f.cardid=l.cardid and f.DateTime=l.scan_off_DateTime
left
join stg.calendar cal on cal.traveldate = n.TravelDate
left
join stg.StopLocation slon on slon.StopLocationId = n.stopid
left 
join stg.StopLocation slof on slof.StopLocationId = f.stopid
left
join stg.CardSubType con on con.CardSubTypeID = n.CardType
order by traveldate, cardid, scan_on_DateTime
; 


select count(*)
from model2.joined_card_scans_on_TravelDate
;-- 795,151,116
select count(*)
from model2.flat_joined_scans2
;-- 785,291,448

select top 999 *
from model2.joined_card_scans_on_TravelDate
;
select top 999 *
from model2.flat_joined_scans2
;

-- save this to s3 for posible use with athena, or emr spark
unload ('select * from model2.flat_joined_scans2')   
to 's3://databridge-syd/joined_scans/journey-' 
iam_role 'arn:aws:iam::579300331566:role/RedshiftReadFromS3'
allowoverwrite
parallel on
maxfilesize 800 mb
delimiter '|' gzip;

