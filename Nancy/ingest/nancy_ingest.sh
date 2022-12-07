hdfs dfs -mkdir project
hdfs dfs -mkdir project/nancy
hdfs dfs -mkdir project/nancy/input
hdfs dfs -mkdir project/nancy/output

hdfs dfs -put /home/ym1581/project/input/nancyma/Daily_Traffic_on_MTA_Bridges___Tunnels.csv hdfs://horton.hpc.nyu.edu:8020/user/ym1581/project/nancy/input
