#Hbase table operation

env:
* Hbase-2.1.2
* java8
* dajobe/hbase

step:

1. `docker pull dajobe/hbase`
2. `docker run -d -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301  -p 16030:16030 -p 16020:16020 --name hbase001 dajobe/hbase`
3. run this project