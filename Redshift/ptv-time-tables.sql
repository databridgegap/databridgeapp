drop table if exists stg_tt.service_calendar;
create table stg_tt.service_calendar(
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

copy stg_tt.service_calendar from 's3://databridge-ptv-timetables/calendar/calendar' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;

drop table if exists stg_tt.calendar_dates;
create table stg_tt.calendar_dates
(
	service_id varchar(28) primary key references stg_tt.service_calendar(service_id) encode lzo,
	date date encode lzo,
	exception_type smallint encode lzo
)
distkey (service_id)
sortkey (service_id);

copy stg_tt.calendar_dates
from 's3://databridge-ptv-timetables/calendar_dates/calendar_dates' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;

drop table if exists stg_tt.routes;
create table stg_tt.routes
(
	route_id varchar(28) primary key encode lzo,
	agency_id varchar(4) encode lzo,
	route_short_name varchar(20) encode lzo,
	route_long_name varchar(124) encode lzo,
	route_type smallint encode lzo
)
distkey (route_id)
sortkey (route_id);

copy stg_tt.routes
from 's3://databridge-ptv-timetables/routes/routes' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;

drop table if exists stg_tt.shapes;
create table stg_tt.shapes
(
	shape_id varchar(28) primary key encode lzo,
	shape_pt_lat float8 encode zstd,
	shape_pt_lon float8 encode zstd,
	shape_pt_sequence smallint encode zstd,
	shape_dist_traveled float8 encode zstd
)
distkey (shape_id)
sortkey (shape_id);

copy stg_tt.shapes
from 's3://databridge-ptv-timetables/shapes/shapes' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;

drop table if exists stg_tt.stops;
create table stg_tt.stops
(
	stop_id integer primary key encode lzo,
	stop_name varchar(124) encode lzo,
	stop_lat float8 encode zstd,
	stop_lon float8 encode zstd
)
distkey (stop_id)
sortkey (stop_id);

copy stg_tt.stops
from 's3://databridge-ptv-timetables/stops/stops' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;

drop table if exists stg_tt.trips;
create table stg_tt.trips
(
	route_id varchar(28) references stg_tt.routes (route_id) encode lzo,
	service_id varchar(28) references stg_tt.service_calendar(service_id) encode lzo,
	trip_id varchar(28) primary key encode lzo,
	shape_id varchar(28) references stg_tt.shapes (shape_id) encode lzo,
	trip_headsign varchar(60) encode lzo,
	direction_id smallint encode lzo
)
distkey (route_id)
sortkey (route_id);

copy stg_tt.trips
from 's3://databridge-ptv-timetables/trips/trips' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;

drop table if exists stg_tt.stop_times;
create table stg_tt.stop_times
(
	trip_id varchar(28) references stg_tt.trips (trip_id) encode lzo,
	arrival_time char(8) encode lzo,
	departure_time char(8) encode lzo,
	stop_id integer encode lzo,
	stop_sequence smallint encode lzo,
	stop_headsign varchar(28) encode lzo,
	pickup_type smallint encode lzo,
	drop_off_type smallint encode lzo,
	shape_dist_traveled float8 encode zstd
)
distkey (trip_id)
sortkey (trip_id);

copy stg_tt.stop_times
from 's3://databridge-ptv-timetables/stop_times/stop_times' 
iam_role '<<Role for Redshift to fead from S3>>'
dateformat 'yyyymmdd'
ignoreblanklines
ignoreheader 1
format csv gzip
;


vacuum stg_tt.service_calendar to 100 percent;
vacuum stg_tt.calendar_dates to 100 percent;
vacuum stg_tt.shapes to 100 percent;
vacuum stg_tt.stops to 100 percent;
vacuum stg_tt.trips to 100 percent;

set analyze_threshold_percent to 0;
analyze;




