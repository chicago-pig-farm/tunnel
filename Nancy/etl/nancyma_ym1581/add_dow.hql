
DROP TABLE IF EXISTS tunnels;

CREATE EXTERNAL TABLE IF NOT EXISTS tunnels (`date` STRING, plaza INT, name STRING, boro STRING, vehicles STRING, dayofWK INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

SHOW TABLES;

LOAD DATA inpath 'hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/nancy/output/part-r-00000' overwrite into table tunnels;

DROP TABLE IF EXISTS tunnel;

CREATE TABLE tunnel as 
SELECT substr(`date`,7) as year, plaza, boro, sum(vehicles) as tunnel, 'yes' as weekday
FROM tunnels
WHERE  from_unixtime(unix_timestamp(`date`,'MM/dd/yyyy'),'u') < 6
GROUP BY substr(`date`,7), plaza, boro
UNION
SELECT substr(`date`,7) as year, plaza, boro, sum(vehicles) as tunnel, 'no' as weekday
FROM tunnels
WHERE  from_unixtime(unix_timestamp(`date`,'MM/dd/yyyy'),'u') >5
GROUP BY substr(`date`,7), plaza, boro;



DROP TABLE IF EXISTS tunnels2;

create table tunnels2 as
select year, boro, tunnel, weekday from tunnel where plaza not in(21,22) UNION
select year, 'M' as boro, tunnel/2 as tunnel, weekday from tunnel where plaza=21 UNION
select year, 'Q' as boro, tunnel/6 as tunnel, weekday from tunnel where plaza=21 UNION
select year, 'Bx' as boro, tunnel/3 as tunnel, weekday from tunnel where plaza=21 UNION
select year, 'Bx' as boro, tunnel/2 as tunnel, weekday from tunnel where plaza=22 UNION
select year, 'Q' as boro, tunnel/2 as tunnel, weekday from tunnel where plaza=22;

Drop table if exists tunnels3;

create table tunnels3 as
select year, boro, sum(tunnel)/260 as tunnnel, weekday from tunnels2 where boro in ('B', 'Bx', 'M', 'Q', 'BX') and weekday='yes'
group by year, boro, weekday
union
select year, boro, sum(tunnel)/104 as tunnnel, weekday from tunnels2 where boro in ('B', 'Bx', 'M', 'Q', 'BX') and weekday='no'
group by year, boro, weekday;


drop table if exists commute;

create table commute as
select a.year, a.boro, round(b.subway,0) as subway, round(a.tunnnel,0) as tunnel, a.weekday
from
 tunnels3 a,
 subway b
where
 a.year=b.year and
 a.boro=b.boro and
 a.weekday=b.weekday;
INSERT OVERWRITE DIRECTORY 'hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/output/combined';