drop table if exists model2.service_calendar;
create table model2.service_calendar(
	service_id varchar(12) primary key encode lzo,
	monday boolean encode zstd,
	tuesday boolean encode zstd,
	wednesday boolean encode zstd,
	thursday boolean encode zstd,
	friday boolean encode zstd,
	saturday boolean encode zstd,
	sunday boolean encode zstd,
	start_date date encode lzo,
	end_date date encode lzo
)
distkey (service_id)
sortkey (service_id);

insert into model2.service_calendar
select * from stg_tt.service_calendar;
/*
copy model2.service_calendar from 's3://databridge-ptv-timetables/calendar/calendar' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;
*/

drop table if exists model2.calendar_dates;
create table model2.calendar_dates
(
	service_id varchar(28) primary key references model2.service_calendar(service_id) encode lzo,
	date date encode lzo,
	exception_type smallint encode lzo
)
distkey (service_id)
sortkey (service_id);

insert into model2.calendar_dates
select * from stg_tt.calendar_dates;
/*
copy model2.calendar_dates
from 's3://databridge-ptv-timetables/calendar_dates/calendar_dates' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;*/

drop table if exists model2.routes;
create table model2.routes
(
	route_id varchar(28) primary key encode lzo,
	agency_id varchar(4) encode lzo,
	route_short_name varchar(20) encode lzo,
	route_long_name varchar(124) encode lzo,
	route_type smallint encode lzo
)
distkey (route_id)
sortkey (route_id);

insert into model2.routes
select * from stg_tt.routes;
/*
copy model2.routes
from 's3://databridge-ptv-timetables/routes/routes' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;*/

drop table if exists model2.shapes;
create table model2.shapes
(
	shape_id varchar(28) primary key encode lzo,
	shape_pt_lat float8 encode zstd,
	shape_pt_lon float8 encode zstd,
	shape_pt_sequence smallint encode zstd,
	shape_dist_traveled float8 encode zstd
)
distkey (shape_id)
sortkey (shape_id);

insert into model2.shapes
select * from stg_tt.shapes;
/*
copy model2.shapes
from 's3://databridge-ptv-timetables/shapes/shapes' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;*/

drop table if exists model2.stops;
create table model2.stops
(
	stop_id integer primary key encode lzo,
	stop_name varchar(124) encode lzo,
	stop_lat float8 encode zstd,
	stop_lon float8 encode zstd
)
distkey (stop_id)
sortkey (stop_id);

insert into model2.stops
select * from stg_tt.stops;
/*
copy model2.stops
from 's3://databridge-ptv-timetables/stops/stops' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;*/

drop table if exists model2.trips;
create table model2.trips
(
	route_id varchar(28) references model2.routes (route_id) encode lzo,
	service_id varchar(28) references model2.service_calendar(service_id) encode lzo,
	trip_id varchar(28) primary key encode lzo,
	shape_id varchar(28) references model2.shapes (shape_id) encode lzo,
	trip_headsign varchar(60) encode lzo,
	direction_id smallint encode lzo
)
distkey (route_id)
sortkey (route_id);

insert into model2.trips
select * from stg_tt.trips;
/*
copy model2.trips
from 's3://databridge-ptv-timetables/trips/trips' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;*/

drop table if exists model2.stop_times;
create table model2.stop_times
(
	trip_id varchar(28) references model2.trips (trip_id) encode lzo,
	arrival_time char(8) encode lzo,
	departure_time char(8) encode lzo,
	stop_id integer references model2.stops (stop_id) encode lzo,
	stop_sequence smallint encode lzo,
	stop_headsign varchar(28) encode lzo,
	pickup_type smallint encode lzo,
	drop_off_type smallint encode lzo,
	shape_dist_traveled float8 encode zstd
)
distkey (trip_id)
sortkey (trip_id);

insert into model2.stop_times
select * from stg_tt.stop_times;
/*
copy model2.stop_times
from 's3://databridge-ptv-timetables/stop_times/stop_times' 
iam_role '<<Role for Redshift to read from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;*/


CREATE TABLE model2.journey
(
	cardid INTEGER DISTKEY encode raw,
	traveldate DATE encode raw,

	calendaryear INTEGER ENCODE lzo,
	monthnumber INTEGER ENCODE lzo,
	calendarmonth VARCHAR(12) ENCODE lzo,
	calendarweek INTEGER ENCODE lzo,
	weekdayseq INTEGER ENCODE lzo,
	weekday VARCHAR(12) ENCODE lzo,
	holidays VARCHAR(28) ENCODE lzo,
	weekending VARCHAR(16) ENCODE lzo,
	absweek INTEGER ENCODE lzo,

	cardtype SMALLINT ENCODE lzo,
	cardsubtypedesc VARCHAR(60) ENCODE lzo,
	paymenttype CHAR(4) ENCODE lzo,
	faretype VARCHAR(10) ENCODE lzo,
	concessiontype VARCHAR(28) ENCODE lzo,
	micardgroup VARCHAR(28) ENCODE lzo,

	scan_on_datetime TIMESTAMP ENCODE lzo,
	scan_off_datetime TIMESTAMP ENCODE lzo,

	scan_on_vehicleid INTEGER ENCODE lzo,
	scan_off_vehicleid INTEGER ENCODE lzo,

	scan_on_mode INTEGER ENCODE lzo,
	scan_on_stopid INTEGER references model2.stops (stop_id) ENCODE lzo,
	scan_on_stoptype VARCHAR(28) ENCODE lzo,
	scan_on_stopnameshort VARCHAR(60) ENCODE lzo,
	scan_on_stopnamelong VARCHAR(100) ENCODE lzo,
	scan_on_suburbname VARCHAR(60) ENCODE lzo,
	scan_on_regionname VARCHAR(60) ENCODE lzo,
	scan_on_gpslat NUMERIC(9, 6) ENCODE lzo,
	scan_on_gpslong NUMERIC(9, 6) ENCODE lzo,

	scan_off_mode INTEGER ENCODE lzo,
	scan_off_stopid INTEGER references model2.stops (stop_id) ENCODE lzo,
	scan_off_stoptype VARCHAR(28) ENCODE lzo,
	scan_off_stopnameshort VARCHAR(60) ENCODE lzo,
	scan_off_stopnamelong VARCHAR(100) ENCODE lzo,
	scan_off_suburbname VARCHAR(60) ENCODE lzo,
	scan_off_regionname VARCHAR(60) ENCODE lzo,
	scan_off_gpslat NUMERIC(9, 6) ENCODE lzo,
	scan_off_gpslong NUMERIC(9, 6) ENCODE lzo,
	
	scan_on_parentroute VARCHAR(16) ENCODE lzo,
	scan_on_routeid INTEGER ENCODE lzo,

	scan_off_parentroute VARCHAR(16) ENCODE lzo,
	scan_off_routeid INTEGER ENCODE lzo
)
sortkey(traveldate, cardid, scan_on_DateTime)
;

insert into model2.journey
select 
	cardid,
	traveldate,
	calendaryear,
	monthnumber,
	calendarmonth,
	calendarweek,
	weekdayseq,
	weekday,
	holidays,
	weekending,
	absweek,
	cardtype,
	cardsubtypedesc,
	paymenttype,
	faretype,
	concessiontype,
	micardgroup,
	scan_on_datetime,
	scan_off_datetime,
	scan_on_vehicleid,
	scan_off_vehicleid,
	scan_on_mode,
	scan_on_stopid,
	scan_on_stoptype,
	scan_on_stopnameshort,
	scan_on_stopnamelong,
	scan_on_suburbname,
	scan_on_regionname,
	scan_on_gpslat,
	scan_on_gpslong,
	scan_off_mode,
	scan_off_stopid,
	scan_off_stoptype,
	scan_off_stopnameshort,
	scan_off_stopnamelong,
	scan_off_suburbname,
	scan_off_regionname,
	scan_off_gpslat,
	scan_off_gpslong,
	scan_on_parentroute,
	scan_on_routeid,
	scan_off_parentroute,
	scan_off_routeid
from model2.flat_joined_scans2
;

vacuum model2.service_calendar to 100 percent;
vacuum model2.calendar_dates to 100 percent;
vacuum model2.shapes to 100 percent;
vacuum model2.stops to 100 percent;
vacuum model2.trips to 100 percent;
vacuum model2.journey to 100 percent;

set analyze_threshold_percent to 0;
analyze;
