#!/bin/sh

JAVA=java

#Define the classpath of Ajira
BASEDIR=`dirname $0`

#Logging dir
CLASSPATH=${BASEDIR}/../conf
for FILE in `find ${BASEDIR}/../ -name '*.jar'`; do
CLASSPATH=${CLASSPATH}:${FILE}
done

#Add additional libs
for FILE in `find ${BASEDIR}/../lib/ -name '*.jar'`; do
CLASSPATH=${CLASSPATH}:${FILE}
done

if [ -z "$AJIRA_OPTS" ]; then
AJIRA_OPTS="-Xmx256m"
fi

JAVA_OPTS="${AJIRA_OPTS} -cp ${CLASSPATH}"

$JAVA $JAVA_OPTS $*