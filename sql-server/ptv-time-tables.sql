drop table if exists routes;
create table routes
(
	route_id varchar(28) primary key,
	agency_id varchar(4),
	route_short_name varchar(20),
	route_long_name varchar(124),
	route_type smallint
);

insert into routes select * from [dbo].[routes-import];
drop table [dbo].[routes-import];

create unique index routes_ix_short_name on routes (route_short_name, route_id);

drop table if exists stops;
create table stops
(
	stop_id integer primary key,
	stop_name varchar(124),
	stop_lat real,
	stop_lon real
);

select distinct * into [dbo].[stops-import2] from [dbo].[stops-import];
drop table [dbo].[stops-import2];
insert into stops select * from [dbo].[stops-import2];
drop table [dbo].[stops-import];

create unique index stops_ix_name on stops (stop_name, stop_id);

drop table if exists trips;
create table trips
(
	route_id varchar(28) references routes (route_id),
	service_id varchar(28),
	trip_id varchar(28) primary key,
	shape_id varchar(28),
	trip_headsign varchar(60),
	direction_id smallint
);

insert into trips select * from [dbo].[trips-import];
drop table [dbo].[trips-import];

create unique index trips_ix_heading on trips (trip_headsign, trip_id) include (route_id);
create unique index trips_ix_route_id on trips (route_id, trip_headsign, direction_id, trip_id);
create unique index trips_ix_route_heading on trips (route_id, trip_id) include (trip_headsign, direction_id);

drop table if exists stop_times;
create table stop_times
(
	trip_id varchar(28) references trips (trip_id),
	arrival_time char(8),
	departure_time char(8),
	stop_id integer,
	stop_sequence smallint,
	stop_headsign varchar(28),
	pickup_type smallint,
	drop_off_type smallint,
	shape_dist_traveled real,
	primary key (trip_id, stop_id, arrival_time)
);

insert into stop_times select * from [dbo].[stop_times-import];
drop table [dbo].[stop_times-import];

create unique index stop_times_ix_1 on stop_times (stop_id, arrival_time, trip_id);
create unique index stop_times_ix_2 on stop_times (trip_id, stop_sequence) include (stop_id, arrival_time);
