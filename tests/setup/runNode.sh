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
bin/elasticsearch

