create schema model1;

create or replace view model1.stop as
select * from stg.StopLocation
;

create or replace view model1.cardType as
select * from stg.CardSubType
;

create or replace view model1.calendar as
select * from stg.calendar
;

create table stg.scan (
 ScanType char(2) encode raw
,Mode int  encode lzo 
,TravelDate date references stg.calendar (TravelDate)  encode raw
,DateTime timestamp  encode lzo
,CardID int encode lzo
,CardType int references stg.CardSubType ( CardSubTypeID )  encode mostly8
,VehicleID int  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int references stg.StopLocation (StopLocationID) encode raw
)
distkey (StopID)
sortkey (ScanType, TravelDate, StopID)
;

insert into stg.scan
select 'on' as ScanType, a.* from stg.ScanOn a
union all
select 'of' as ScanType, b.* from stg.ScanOff b
;

create or replace view model1.scan as
select * from stg.scan
;

