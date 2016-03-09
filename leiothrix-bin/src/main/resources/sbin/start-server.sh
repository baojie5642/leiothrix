#!/usr/bin/env bash

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/aliyun/server/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=${BASE_DIR}/lib:${CLASSPATH}
echo "Starting server..."

#===========================================================================================
# JVM Configuration
#===========================================================================================
if [ ! -d $BASE_DIR/logs ]; then
  mkdir $BASE_DIR/logs
fi

JAVA_OPT="${JAVA_OPT} -server -Xms256m -Xmx1024m -XX:PermSize=64m -XX:MaxPermSize=256m"
JAVA_OPT="${JAVA_OPT} -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:${BASE_DIR}/logs/leiothrix_server_gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib"
#JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"
LOG_OPT="-Dlogback.configurationFile=$BASE_DIR/conf/logback.xml"
CONFIG_OPT="-Dconfig.file=$BASE_DIR/conf/config.properties"

$JAVA ${JAVA_OPT} ${CONFIG_OPT} ${LOG_OPT} xin.bluesky.leiothrix.server.Server  2>&1 &
result=$?
echo $! > PID
if [ $result -eq 0 ]
then
  sleep 2
fi
echo "Log file is in $HOME/logs directory"
exit $result