# PXF Kafka plugin

[PXF](https://gpdb.docs.pivotal.io/6-8/pxf/overview_pxf.html) plugin to write data to Kafka topic.

## How to deploy and use

You should have working GPDB or ADB cluster with installed PXF.

### BUILD

```shell script
./gradlew clean build jar
```

### INSTALL
You will get a `pxf-kafka/build` folder with target jars. Copy them to each GPDB or ADB host and delete the old one:

```shell script
cp pxf-kafka.jar /usr/lib/pxf/lib
chown pxf:pxf /usr/lib/pxf/lib/pxf-kafka.jar
rm /usr/lib/pxf/lib/shared/avro-1.7.7.jar
cp avro-1.9.2.jar /usr/lib/pxf/lib/shared/
chown pxf:pxf /usr/lib/pxf/lib/shared/avro-1.9.2.jar
```
Then sync greenplum cluster
```shell script
pxf cluster sync
```
### CHECK
From GPDB SQL console try to execute:

```sql
CREATE WRITABLE EXTERNAL TABLE kafka_tbl (a TEXT, b TEXT, c TEXT)
  LOCATION ('pxf://<topic>?PROFILE=kafka&BOOTSTRAP_SERVERS=<server>')
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

insert into kafka_tbl values ('a', 'b,c', 'd'), ('x', 'y', 'z');

drop external table kafka_tbl;
```
