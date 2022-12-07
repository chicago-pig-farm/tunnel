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

insert overwrite directory 'hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/output' 
row format delimited fields terminated by ',' 
select * from commute;

