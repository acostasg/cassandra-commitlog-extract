#!/bin/bash

source `dirname $0`/common.sh

# param 1: absolute path to the conf folder
# param 2: name of the exporter to run
java -server -Xmx512m -cp $CLASSPATH -Dlog4j.configuration=file:$AGENT_HOME/log4j/incremental.properties agent.Main $AGENT_HOME/conf incremental-csv-single $1
exit $?
