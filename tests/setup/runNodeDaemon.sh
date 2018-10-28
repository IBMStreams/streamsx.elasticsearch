#!/bin/bash

nodeNum=${1:-"1"}
version=${2:-${ELA_VERSION}}
if [ "$version" == "" ] ; then
	version="6.2.2"
fi
serverBaseDir=server${version}_
serverDir=$serverBaseDir$nodeNum

# if ELA_JAVA_HOME is set, use it as JAVA_HOME
# set this, if your default JAVA is too old (eg. Streams 4.2 Java), and you installed a newer version elsewhere
# the JAVA version must be 1.8, patchlevel greater than 131
if [ "$ELA_JAVA_HOME" != "" ] ; then
export JAVA_HOME="$ELA_JAVA_HOME"
fi

# start server 
cd $serverDir
bin/elasticsearch -d -p pid.txt

# check health, retry for 20 seconds
running="false"
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
do
#	echo try : $i 
	status=`curl -s -S -X GET 'http://localhost:9200/_cluster/health?pretty' | grep -c green`
	if [ "$status" == "1" ] ; then
		running="true"
		break
	fi
	sleep 1
done

if [ "$running" != "true" ] ; then
	echo "es cluster is not healthy after $i seconds"
	exit 1
fi

echo "es cluster is healthy"
exit 0

