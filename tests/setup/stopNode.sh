#!/bin/bash

nodeNum=${1:-"1"}
version=${2:-${ELA_VERSION}}
if [ "$version" == "" ] ; then
	version="6.2.2"
fi
serverBaseDir=server${version}_
serverDir=$serverBaseDir$nodeNum

# stop server 
cd $serverDir
kill `cat pid.txt`

