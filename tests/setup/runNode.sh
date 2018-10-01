#!/bin/bash

version=$ELA_VERSION
serverBaseDir=server${version}_
nodeNum=${1:-"1"}
serverDir=$serverBaseDir$nodeNum

# start server 
cd $serverDir
bin/elasticsearch




