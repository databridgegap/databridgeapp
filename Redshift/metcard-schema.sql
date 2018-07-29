create schema stg;

create table stg.StopLocation
(
 StopLocationID int  encode raw
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
 CardSubTypeID int  encode zstd
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
drop table stg.calendar;
create table stg.calendar
(
 DateId int  encode lzo
,TravelDate date  encode raw
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
20170930|2017-09-30|2017|FY2017 - 2018|9|September|201709|2017Q3|FY17-18Q1|39|13|Saturday|Weekend|Saturday|6|Saturday|201709|Sep 17/|9|1239|w/e 2017-09-30|September Qtr. 2017
20080615|2008-06-15|2008|FY2007 - 2008|18|June|200806|2008Q2|FY07-08Q4|24|50|Sunday|Weekend|Sunday|7|Sunday|200818|Jun 07/|6|755|w/e 2008-06-21|June Qtr. 2008
20040222|2004-02-22|2004|FY2003 - 2004|14|February|200402|2004Q1|FY03-04Q3|8|34|Sunday|Weekend|Normal Sunday|7|Sunday|200414|Feb 03/|2|530|w/e 2004-02-28|March Qtr. 2004
20190620|2019-06-20|2019|FY2018 - 2019|18|June|201906|2019Q2|FY18-19Q4|24|51|Weekday|Weekday|0|4|Thursday|201906|Jun 18/|6|1329|w/e 2019-06-22|June Qtr. 2019
;

copy stg.calendar
from 's3://databridge-syd/calendar.txt' 
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
;
;
select starttime, err_code, err_reason, colname, raw_field_value
;select *
from stl_load_errors
order by starttime desc
;


create table stg.ScanOn (
 Mode int  encode lzo 
,TravelDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType int  encode mostly8
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
,BusinessDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType int  encode mostly8
,VehicleID int  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int  encode raw
)
distkey (StopID)
sortkey (BusinessDate, StopID)
;

truncate table stg.ScanOff;
copy stg.ScanOff
from 's3://databridge-syd/scanoff.manifest.json' manifest
iam_role '<<arn of role not included because this is a public repository>>'
delimiter '|'
TRUNCATECOLUMNS
gzip
;

create schema refined;

create table refined.scanoffon
(
 Mode int  encode lzo 
,BusinessDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType int  encode mostly8
,VehicleID int  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int  encode raw
,onoff varchar(3) encode raw 
)
distkey (StopID)
sortkey (BusinessDate, onoff, StopID)
;
