create schema raw;

create table raw.StopLocation
(
 StopLocationID int  encode raw
,StopNameShort varchar(60)  encode zstd
,StopNameLong varchar(100)  encode zstd
,StopType varchar(28)  encode raw
,SuburbName varchar(60)  encode raw
,PostCode smallint  encode zstd
,RegionName varchar(60)  encode zstd
,LocalGovernmentArea varchar(60)  encode zstd
,StatDivision varchar(28)  encode zstd
,GPSLat decimal(3, 6)  encode zstd
,GPSLong decimal(3, 6)  encode zstd
)
distkey (StopLocationID)
sortkey (StopType, SuburbName)
;

create table raw.CardSubType
(
 CardSubTypeID smallint  encode zstd
,CardSubTypeDesc varchar(60)  encode zstd
,PaymentType char(4)  encode zstd
,FareType varchar(10)  encode zstd
,ConcessionType varchar(28)  encode zstd
,MICardGroup varchar(28)  encode zstd
)
diststyle even
sortkey(FareType, PaymentType, CardSubTypeDesc)
;

create table raw.calendar
(
 DateId int  encode lzo
,TravelDate date  encode raw
,CalendarYear smallint  encode runlength
,FinancialYear varchar(12) encode runlength
,FinancialMonth int encode runlength
,CalendarMonth varchar(12) encode runlength
,CalendarMonthSeq int encode runlength
,CalendarQuarter varchar(12) encode runlength 
,FinancialQuarter varchar(12) encode runlength
,CalendarWeek int encode runlength
,FinancialWeek int encode runlength
,DayType varchar(12) encode bytedict
,DayTypeCategory varchar(28) encode zstd
,WeekdaySeq shortint encode zstd
,WeekDay varchar(12) encode bytedict
,FinancialMonthSeq shortint encode zstd
,FinancialMonthName varchar(12) encode zstd
,MonthNumber shortint encode zstd
,ABSWeek shortint encode zstd
,WeekEnding varchar(16) encode zstd
,QuarterName varchar(28) encode zstd
)
diststyle even
sortkey (TravelDate, CalendarYear)
;

create table raw.ScanOn (
 Mode smallint  encode lzo 
,TravelDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType smallint  encode mostly8
,VehicleID smallint  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int  encode raw
)
distkey (StopID)
sortkey (TravelDate, StopID)
;

create table raw.ScanOff (
 Mode smallint  encode lzo 
,BusinessDate date  encode raw
,DateTime timestamp  encode lzo
,CardID int  encode lzo
,CardType smallint  encode mostly8
,VehicleID smallint  encode zstd
,ParentRoute varchar(16)  encode zstd
,RouteID int  encode lzo
,StopID int  encode raw
)
distkey (StopID)
sortkey (BusinessDate, StopID)
;

