-- creat an external schema based on the s3 files scaned by the glue crawlers
-- these data are from the pvt time-table avaliable from data.vic.gov.au
drop schema tt;
create external schema gtt -- glue's time-table data
from data catalog database 'ptv-timetables'
iam_role 'arn:aws:iam::579300331566:role/RedshiftReadFromS3';

select * from svv_external_schemas;
select * from svv_external_tables;
select * from SVL_S3LOG;

select 
	'drop table if exists tt.'||tablename||'; '||
	'create table tt.'||tablename||' as '||
	'select distinct * from gtt.'||tablename||';'
from svv_external_tables;

create schema tt; -- timetable data in redshift

drop table if exists tt.agency; create table tt.agency as select distinct * from gtt.agency;
drop table if exists tt.calendar; create table tt.calendar as select distinct * from gtt.calendar;
drop table if exists tt.routes; create table tt.routes as select distinct * from gtt.routes;
drop table if exists tt.shapes; create table tt.shapes as select distinct * from gtt.shapes;
drop table if exists tt.stop_times; create table tt.stop_times as select distinct * from gtt.stop_times;
drop table if exists tt.stops; create table tt.stops as select distinct * from gtt.stops;
drop table if exists tt.trips; create table tt.trips as select distinct * from gtt.trips;
