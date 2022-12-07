javac -classpath "$(yarn classpath)" -d . CountRecsMapper.java
javac -classpath "$(yarn classpath)" -d . CountRecsReducer.java
javac -classpath "$(yarn classpath)":. -d . CountRecs.java

jar -cvf countrecs.jar *.class

hdfs dfs -rm -r -f project/nancy/output/countrecs

hadoop jar countrecs.jar CountRecs project/nancy/input/Daily_Traffic_on_MTA_Bridges___Tunnels.csv project/nancy/output/countrecs

hdfs dfs -cat project/nancy/output/countrecs/part-r-00000
