#!/bin/bash

source `dirname $0`/common.sh

tipo=reader-csv-single-oracle
csvdir=/cassandra/etl-agent/oracle
pidfile=/var/run/etlagentOracle.pid
logfile=/var/log/etl/read-csv-single-oracle.log
javapath=/usr/java/jre1.7.0_21/bin/java
statusfile=/cassandra/etl-agent/agent/status/statusOracle.conf
now=`date`
# param 1: absolute path to the conf folder
# param 2: name of the exporter to run

status=`md5sum $logfile`
echo "El md5sum del log es $status. Me espero 30 segundos."
sleep 30
status2=`md5sum $logfile`
echo "Ahora el md5sum del log es $status2"

[ "$status" != "$status2" ] && echo "Como son diferentes, hay un proceso en marcha que escribe en el log, mejor no me meto. Saliendo" && exit 0

while [ -f $pidfile ];do 
  echo "Nadie escribe en el log y hay un pidfile... Voy a ver si el proceso esta en marcha"
  PID=`cat $pidfile`
  if [ -e /proc/$PID -a /proc/$PID/exe -ef $javapath ]; then
    echo "Esta en marcha. Seguramente esta colgado, asi que lo mato." 
    kill `cat $pidfile`
    sleep 10
    if [ -e /proc/$PID -a /proc/$PID/exe -ef $javapath ]; then
      echo "Aun sigue en marcha, esta muy colgado... le meto un kill -9"
      kill -9 `cat $pidfile`
    fi
    now=`date`
    echo "\"0\",\"$now\"" > $statusfile
  else
    echo "No esta en marcha. Borro el pidfile."
  fi
  rm -f $pidfile
  echo "Como habia un pidfile, he hecho gestiones para arreglar el problema pero no proceso csv's. Si todo ha ido bien, la siguiente ejecucion los procesara a toda pastilla"
  exit 0
done  

while [ -n "`ls $csvdir/*csv`" ];do 
  sleep 1
  [ "`cat $statusfile|cut -d'\"' -f2`" == "2" ] && tail -n 1000 $logfile > /var/log/etl/read.log && mail -s "La ETL de $csvdir no quiere funcionar, pongo el status a 0 y aqui no paso nada" etl@groupalia.com < /var/log/etl/read.log && :> /var/log/etl/reader.log && :> /var/log/etl/read.log && echo "\"0\",\"$now\"" > $statusfile  &&  echo "Ha habido un error segun el statusfile" && exit 1
  [ "`cat $statusfile|cut -d'\"' -f2`" == "1" ] && echo "Segun el fichero de control de java, hay otro proceso corriendo, pero yo no lo he visto... Algo pasa, yo salgo por si acaso" && exit 0
  
  csvnumber=`ls $csvdir/*csv|wc -l`
  echo "Number of csv: $csvnumber"
  java -server -Xmx2048m -cp $CLASSPATH -Dlog4j.configuration=file:$AGENT_HOME/log4j/read-csv-single-oracle.properties agent.Main $AGENT_HOME/conf $tipo
done

