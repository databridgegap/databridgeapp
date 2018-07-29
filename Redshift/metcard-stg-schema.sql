create schema stg;

create table stg.StopLocation
(
 StopLocationID int primary key encode raw
,StopNameShort varchar(60)  encode zstd
,StopNameLong varchar(100)  encode zstd
,StopType varchar(28)  encode raw
,SuburbName varchar(60)  encode raw
,PostCode int  encode zstd
,RegionName varchar(60)  encode zstd
,LocalGovernmentArea varchar(60)  encode zstd
,StatDivision varchar(28)  encode zstd
,GPSLat decimal(9, 6)  encode zstd
,GPSLong decimal(9, 6)  encode zstd
)
distkey (StopLocationID)
sortkey (StopType, SuburbName)
;
	
copy stg.StopLocation
from 's3://databridge-syd/stop_locations.txt' 
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
;

select count(*) from stg.StopLocation
;

create table stg.CardSubType
(
 CardSubTypeID int primary key encode zstd
,CardSubTypeDesc varchar(60)  encode zstd
,PaymentType char(4)  encode zstd
,FareType varchar(10)  encode zstd
,ConcessionType varchar(28)  encode zstd
,MICardGroup varchar(28)  encode zstd
)
diststyle all
sortkey(FareType, PaymentType, CardSubTypeDesc)
;

copy stg.CardSubType
from 's3://databridge-syd/card_types.txt' 
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
;

select * from stg.CardSubType;
;


create table stg.calendar
(
 DateId int  encode lzo
,TravelDate date primary key encode raw
,CalendarYear int  encode runlength
,FinancialYear varchar(16) encode runlength
,FinancialMonth int encode runlength
,CalendarMonth varchar(12) encode runlength
,CalendarMonthSeq int encode runlength
,CalendarQuarter char(6) encode runlength 
,FinancialQuarter varchar(20) encode runlength
,CalendarWeek int encode runlength
,FinancialWeek int encode runlength
,WeekDayorNot varchar(12) encode bytedict
,DayTypeCategory varchar(28) encode zstd
,Holidays varchar(28) encode zstd
,WeekdaySeq int encode zstd
,WeekDay varchar(12) encode bytedict
,FinancialMonthSeq int encode zstd
,FinancialMonthName varchar(12) encode zstd
,MonthNumber int encode zstd
,ABSWeek int encode zstd
,WeekEnding varchar(16) encode zstd
,QuarterName varchar(28) encode zstd
)
diststyle all
sortkey (TravelDate, CalendarYear)
;

copy stg.calendar
from 's3://databridge-syd/calendar.txt' 
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
;

select starttime, err_code, err_reason, colname, raw_field_value
from stl_load_errors
order by starttime desc
limit 5
;

set slot wlm_query_slot_count to 2;
vacuum stg.StopLocation to 100 percent;
vacuum stg.CardSubType to 100 percent;
vacuum stg.calendar to 100 percent;


create table stg.ScanOn (
 Mode int  encode lzo 
,TravelDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType smallint  encode mostly8
,VehicleID int  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int  encode raw
)
distkey (StopID)
sortkey (TravelDate, StopID)
;

copy stg.ScanOn
from 's3://databridge-syd/scanon.manifest.json' manifest
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
gzip
;


create table stg.ScanOff (
 Mode int  encode lzo 
,TravelDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType smallint  encode mostly8
,VehicleID int  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int  encode raw
)
distkey (StopID)
sortkey (TravelDate, StopID)
;

copy stg.ScanOff
from 's3://databridge-syd/scanoff.manifest.json' manifest
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
gzip
;

