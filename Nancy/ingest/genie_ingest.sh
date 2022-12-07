hdfs dfs -mkdir project/genie
hdfs dfs -mkdir project/genie/input
hdfs dfs -mkdir project/genie/output


hdfs dfs -put /home/ym1581/project/input/geniehou/2021AverageWeekdayRidership.csv hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/input
hdfs dfs -put /home/ym1581/project/input/geniehou/2021AverageWeekendRidership.csv hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/genie/input
