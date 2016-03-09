#!/usr/bin/env bash

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/aliyun/server/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=${BASE_DIR}/conf:${BASE_DIR}/lib:${CLASSPATH}

echo "Cancel task..."

#===========================================================================================
# JVM Configuration
#===========================================================================================
JAVA_OPT="${JAVA_OPT} -server -Xms256m -Xmx512m -XX:PermSize=64m -XX:MaxPermSize=256m"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"
LOG_OPT="-Dlogback.configurationFile=$BASE_DIR/conf/logback.xml"

$JAVA ${JAVA_OPT} ${LOG_OPT} xin.bluesky.leiothrix.bin.CancelTaskBin $1 $2 $3