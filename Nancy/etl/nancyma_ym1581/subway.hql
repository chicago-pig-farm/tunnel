DROP TABLE IF EXISTS subway_weekday;


CREATE EXTERNAL TABLE IF NOT EXISTS subway_weekday (Station STRING,empty STRING,Boro STRING,y2016 STRING,y2017 STRING,y2018 STRING,y2019 STRING,y2020 STRING,y2021 STRING,C2020 STRING,empty2 STRING,y2021R STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES("seperatorChar"=",","quoteChar"="\"")
STORED AS TEXTFILE;

ALTER TABLE subway_weekday
SET TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA inpath 'hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/input/2021AverageWeekdayRidership.csv' overwrite into table subway_weekday;

Drop table if exists subway_weekday_2;

create table subway_weekday_2 as
select boro, '2019' as year, replace(y2019,',','') as subway, 'yes' as weekday from subway_weekday where boro not in ('Boro','') union
select boro, '2020' as year, replace(y2020,',','') as subway, 'yes' as weekday from subway_weekday where boro not in ('Boro','') union
select boro, '2021' as year, replace(y2021,',','') as subway, 'yes' as weekday from subway_weekday where boro not in ('Boro','');

select boro, year, sum(subway) as subway, weekday from subway_weekday_2 group by boro, year, weekday;


DROP TABLE IF EXISTS subway_weekend;

CREATE EXTERNAL TABLE IF NOT EXISTS subway_weekend (Station STRING,empty STRING,Boro STRING,y2016 STRING,y2017 STRING,y2018 STRING,y2019 STRING,y2020 STRING,y2021 STRING,C2020 STRING,empty2 STRING,y2021R STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES("seperatorChar"=",","quoteChar"="\"")
STORED AS TEXTFILE;

ALTER TABLE subway_weekend
SET TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA inpath 'hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/input/2021AverageWeekendRidership.csv' overwrite into table subway_weekend;

Drop table if exists subway_weekend_2;

create table subway_weekend_2 as
select boro, '2019' as year, replace(y2019,',','') as subway, 'no' as weekday from subway_weekend where boro not in ('Boro','') union
select boro, '2020' as year, replace(y2020,',','') as subway, 'no' as weekday from subway_weekend where boro not in ('Boro','') union
select boro, '2021' as year, replace(y2021,',','') as subway, 'no' as weekday from subway_weekend where boro not in ('Boro','');

drop table if exists subway;

create table subway as 
select boro, year, sum(subway) as subway, weekday from subway_weekday_2 group by boro, year, weekday UNION

select boro, year, sum(subway) as subway, weekday from subway_weekend_2 group by boro, year, weekday;
