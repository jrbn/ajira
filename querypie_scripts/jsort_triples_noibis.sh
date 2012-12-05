#!/bin/bash

JAR_FILE="jar/querypie.jar"
JVM_ARGS="-Xmx256m -Dibis.pool.name=sorting -Dibis.server.address=localhost -Dibis.pool.size=1 -Dlogback.configurationFile=conf/logback.xml" 
INPUT="/home/mocke/MasterProj/lubm_1" 
OUTPUT="/home/mocke/MasterProj/out"
MAIN_CLASS="BenchmarkSorting"

./clean_log.sh

java $JVM_ARGS -jar $JAR_FILE $INPUT $OUTPUT 1 --storage-engine files --without-ibis 

less log/$HOSTNAME/output.log

