#!/bin/bash

version=${1:-${ELA_VERSION}}
if [ "$version" == "" ] ; then
	version="6.2.2"
fi
serverDir1=server${version}_1
serverDir2=server${version}_2

# remove directories
rm -rf $serverDir1 $serverDir2

