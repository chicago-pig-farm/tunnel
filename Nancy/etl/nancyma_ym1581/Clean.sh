hdfs dfs -rm -r -f /user/ym1581/project/nancy/output
rm *.class
rm *.jar
rm project/nancy/output/part-r-00000

javac -classpath "$(yarn classpath)" -d . CleanMapper.java
javac -classpath "$(yarn classpath)" -d . CleanReducer.java
javac -classpath "$(yarn classpath)":. -d . Clean.java

jar -cvf clean.jar *.class
hadoop jar clean.jar Clean project/nancy/input/Daily_Traffic_on_MTA_Bridges___Tunnels.csv project/nancy/output

hdfs dfs -get project/nancy/output/part-r-00000 /home/ym1581/project/etl_code/nancyma_ym1581
