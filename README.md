# tunnel
My main job in the project was handling my tunnel dataset, transforming the subway dataset so the two datasets are compatible. Then my task was to join both my teammate dataset with mine. My teammate was responsible for performing the overall analytics to help demonstrate trends

DESCRIPTION OF DIRECTORIES
ana_code
    -ana_code/subway: 3 folders that contain analysis of subway data only. looks year YoY trends and changes
    -ana_code/joint: 5 folders that contain analysis of the joint subway and tunnel data
    -ana_code/000000_0: file output of joining subway and tunnel dataset
data_ingest
    data_ingest/genie
      -data_ingest/genie/ingest: contain the .hql file for my teammates data ingestion
      -data_ingest/genie/genie_ingest.sh: executable for reading in data from my teammate for joining
    data_ingest/nancy_ingest.sh: executable for reading in my tunnel data

etl_code
    -etl_code/geniehou_ch3801: my teammates etl code
    -etl_code/nancyma_ym1581: my etl which contains 
        1. MapReduce transformation of tunnel dataset
        2. Hive transformation of tunnel dataset
        3. Hive transformatioin of subway dataset (teammate data)
        4. Hive join of tunnel and subway dataset
input
    -input/geniehou: all of the csv my teammate provided to me
    -input/nancyma: my original input csv 
profiling_code
    -profiling_code/geniechou_ch3801: profiling code by my teammate on subway dataset
    -profiling_code/nancyma_ym1581: my profiling code on tunnel dataset
readme


DATA INGESTION
0. upload all datasets from local to peel using scp
1. run executable nancy_ingest.sh in /home/ym1581/project/data_ingest/nancy_ingest.sh to load tunnel data into hdfs
2. run executable genie_ingest.sh in /home/ym1581/project/data_ingest/genie_ingest.sh to load subway data into hdfs so I can combined the two datasets 

PROFILING
3. run executable CountRecs.sh in /home/ym1581/project/profiling_code/nancyma_ym1581/CountRecs.sh to get see how many records. 
   Output in HDFS at project/nancy/output/countrecs/part-r-00000

ETL/Cleaning/Prep for Analysis 
4. run execetable Clean.sh in /home/ym1581/project/etl_code/nancyma_ym1581/Clean.sh
   This step removes data for years we are not interested in (everything except for 2019-2021). 
   Also, it adds corresponding borough for each entrance, which is necessary to make it compatible with subway dataset during analysis.
   Output in HDFS at project/nancy/output, also copied out of HDFS into /home/ym1581/project/etl_code/nancyma_ym1581/part-r-00000 for viewing and double checking that the code works correctly
5. Connect to hive and beeline for hive queries to create new tables to combine with subway data. 
6. Within beeline, execute the add_dow.hql script at /home/ym1581/project/etl_code/nancyma_ym1581/add_dow.hql (command is !run add_dow.hql). 
   This script does the following:
    - Create hive table 'tunnels' from output of the output of Clean.sh (MR cleaning). This essentially just reads data into Hive
    - Create Hive table 'tunnle' from 'tunnels', this performs two jobs:
        1. convert date to year by using substring and splitting by index (input date formatted as mm/dd/yyyy so split at index 7 to get year only)
        2. assigned each day its day of the week, 1 for monday, 7 for sunday. This will allow me to assign each row a boolean value for weekday (true if 1-5, false if 6-7)
        Both are necessary for comparison to subway data
    - Create hive table 'tunnels2'from 'tunnel'. This table handles the exception case with plaza 21 and 22. the Triboro bridge has multiple entrances and exits into multiple boroughs. 
      Upon some simple algebra, I figured out that for the triboro bridge, inbound traffic roughly divides into 1/6 Queens, 1/2 Manattan, and 1/3 Bronx. Outbound traffic is split 1/2, 1/2 between Queens and Bronx
    - Create hive table 'tunnels3' from 'tunnels2'. This table performs necessary groupby and filtering. 
        1. Remove the entries involving boroughs not included in subway dataset (Staten Island and Rockways)
        2. Perform division to go from total number of cars on every weekday in a year, to the average weekday (divide by 260 since there are 52 weeks, and 5 week days per week). Same is done for weekend days (divide by 104)
        3. 'tunnels3' should contain the average daily number of vehicle  for every combination of year (2019, 2020, 2021), borough(Brooklyn/B, Bronx/Bx, Queens/Q, Manhattan/M), and weekday(no(weekend day), yes(weekday))
7. Within beeline, execute the subway.hql script at /home/ym1581/project/etl_code/nancyma_ym1581/subway.hql (command is !run subway.hql). 
   This script does the following:
    - Create hive table 'subway_weekday' from the 2021AverageWeekdayRidership.csv ('hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/input/2021AverageWeekdayRidership.csv')
      The numbers within this dataset came with commas delimiting every 3 digits. This was handled by using OpenCSVSerde during the table creation query. This ingestion table also skips the header
    - Create 'subway_weekday_2' from 'subway_weekday'. This table mainly is responsible for dropping all of the unneccesary columns. We also combine the 3 seperate columns of 2019, 2020, and 2021 into one column as year. 
      This table also emoves the commas that delimit every 3 digits with replace (',', '') to make it compatible with tunnel dataset.
      This table contains four columns boro (B, Bx, Q, M), year(2019, 2020, 2021),tunne(vehicle count), and weekday (boolean)
    - Perform the same except with the weekend dataset. Create hive table 'subway_weekend' from the 2021AverageWeekendRidership.csv ('hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/input/2021AverageWeekendRidership.csv')
    - Create 'subway_weekend_2' from 'subway_weekend', same as 'subway_weekday_2'
    - Create hive table 'subway' by joining 'subway_weekday_2' and 'subway_weekend_2'. This will give 6 rows per borough (combinations of year + weekday/weekend)
8. Within beeline, execute the join.hql script at /home/ym1581/project/etl_code/nancyma_ym1581/join.hql (command is !run join.hql). 
   This script will join subway dataset and tunnel dataset so we can perform analysis. In addition to joining the datasets, I also rounded all the numbers so they would be easier to compare. 
   The table will have columns 
    year: 2019, 2020, 2021
    boro: B, Bx, M, Q
    subway: average daily number of subway riders collected by subway turnstile
    tunnel: average daily number of tunnel user collected by EZpass and VToll
    weekday: no(weekend day), yes(weekday )
9. Extract hive table 'commute' for analysis 
    >hdfs dfs -get hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/output/000000_0 /home/ym1581/project/ana_code
