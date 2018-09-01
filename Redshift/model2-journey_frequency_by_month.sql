
CREATE TABLE model2.journey_frequency_by_month
(
	calendaryear INTEGER encode raw,
	monthnumber INTEGER encode raw,
	calendarmonth VARCHAR(12) ENCODE lzo,
	cardid INTEGER ENCODE lzo DISTKEY,
	scan_on_stopid INTEGER  references model2.stops (stop_id) ENCODE lzo,
	scan_off_stopid INTEGER  references model2.stops (stop_id) ENCODE lzo,
	journeys BIGINT encode raw,
	scan_on_stoptype VARCHAR(28) ENCODE lzo,
	scan_on_stopnameshort VARCHAR(60) ENCODE lzo,
	scan_on_suburbname VARCHAR(60) ENCODE lzo,
	scan_on_gpslat NUMERIC(9, 6) ENCODE lzo,
	scan_on_gpslong NUMERIC(9, 6) ENCODE lzo,
	scan_off_stoptype VARCHAR(28) ENCODE lzo,
	scan_off_stopnameshort VARCHAR(60) ENCODE lzo,
	scan_off_suburbname VARCHAR(60) ENCODE lzo,
	scan_off_gpslat NUMERIC(9, 6) ENCODE lzo,
	scan_off_gpslong NUMERIC(9, 6) ENCODE lzo,
	cardtype SMALLINT ENCODE lzo,
	cardsubtypedesc VARCHAR(60) ENCODE lzo,
	paymenttype CHAR(4) ENCODE lzo,
	faretype VARCHAR(10) ENCODE lzo,
	concessiontype VARCHAR(28) ENCODE lzo,
	micardgroup VARCHAR(28) ENCODE lzo,
	travel_days BIGINT ENCODE lzo,
	scan_on_locations BIGINT ENCODE lzo,
	scan_off_locations BIGINT ENCODE lzo,
	scan_on_modes BIGINT ENCODE lzo,
	scan_off_modes BIGINT ENCODE lzo,
	scan_on_routs BIGINT ENCODE lzo,
	scan_off_routs BIGINT ENCODE lzo,
	normal_weekday_journeys BIGINT ENCODE lzo,
	weekend_journeys BIGINT ENCODE lzo,
	school_holiday_weekday_journeys BIGINT ENCODE lzo,
	public_holiday_journeys BIGINT ENCODE lzo
)
SORTKEY (calendaryear,monthnumber,journeys);

insert into model2.journey_frequency_by_month 
select calendaryear, monthnumber, calendarmonth, cardid, scan_on_stopid, scan_off_stopid -- 
	, count(*) journeys 

    , scan_on_stoptype
    , scan_on_stopnameshort
    , scan_on_suburbname
    , scan_on_gpslat
    , scan_on_gpslong

    , scan_off_stoptype
    , scan_off_stopnameshort
    , scan_off_suburbname
    , scan_off_gpslat
    , scan_off_gpslong

	, cardtype
	, cardsubtypedesc
	, paymenttype
	, faretype
	, concessiontype
	, micardgroup

	, count(distinct traveldate) travel_days
	, count(distinct scan_on_stopid) scan_on_locations
	, count(distinct scan_off_stopid) scan_off_locations
	, count(distinct scan_on_mode) scan_on_modes
	, count(distinct scan_off_mode) scan_off_modes
	, count(distinct scan_on_routeid) scan_on_routs
	, count(distinct scan_off_routeid)  scan_off_routs

    , sum(case when holidays='Normal Weekday' then 1 else 0 end)  Normal_Weekday_journeys
    , sum(case when holidays in ('Saturday','Sunday') then 1 else 0 end)  Weekend_journeys
    , sum(case when holidays='School Holiday Weekday' then 1 else 0 end)  School_Holiday_Weekday_journeys
    , sum(case when holidays='Public Holiday' then 1 else 0 end)  Public_Holiday_journeys

from model2.journey j
group by calendaryear, monthnumber, calendarmonth, cardid, scan_on_stopid, scan_off_stopid
    , scan_on_stoptype
    , scan_on_stopnameshort
    , scan_on_suburbname
    , scan_on_gpslat
    , scan_on_gpslong

    , scan_off_stoptype
    , scan_off_stopnameshort
    , scan_off_suburbname
    , scan_off_gpslat
    , scan_off_gpslong

	, cardtype
	, cardsubtypedesc
	, paymenttype
	, faretype
	, concessiontype
	, micardgroup
having journeys >= 4 -- 41 million
;

vacuum model2.journey_frequency_by_month;
analyze model2.journey_frequency_by_month;


-- save this to s3 for posible use with athena, or emr spark
unload ('select * from model2.journey_frequency_by_month')   
to 's3://databridge-syd/joined_scans/journey_frequency_by_month-' 
iam_role 'arn:aws:iam::579300331566:role/RedshiftReadFromS3'
allowoverwrite
parallel on
maxfilesize 800 mb
delimiter '|' gzip;

