#!/bin/sh

JAVA=java
JAVA_OPTS="-Xmx256m"

#Define the classpath
CLASSPATH=../ajira-*.jar
for FILE in `find ../lib/ -name *.jar`; do
CLASSPATH=${CLASSPATH}:$FILE
done

$JAVA $JAVA_OPTS $CLASSPATH $*