#!/bin/bash

version=${1:-${ELA_VERSION}}
if [ "$version" == "" ] ; then
	version="6.2.2"
fi
tarfile=elasticsearch-${version}.tar.gz
origDir=elasticsearch-${version}
serverDir1=server${version}_1
serverDir2=server${version}_2

# load ES software if not present 
if [ ! -f $tarfile ]; then
	wget --progress=dot:mega https://artifacts.elastic.co/downloads/elasticsearch/$tarfile
fi

# stop any nodes if they are still running
./stopNode.sh 1 $version
./stopNode.sh 2 $version

# remove install directories
rm -rf $serverDir1 $serverDir2

# extract file and create 2 copies for cluster usage
tar xzvf $tarfile
mv $origDir $serverDir1
cp -r $serverDir1 $serverDir2

# configure cluster nodes
cfg1=$serverDir1/config/elasticsearch.yml
cfg2=$serverDir2/config/elasticsearch.yml

echo 'cluster.name: cluster1' >>$cfg1
echo 'cluster.name: cluster1' >>$cfg2

echo 'node.name: node1' >>$cfg1
echo 'node.name: node2' >>$cfg2

echo 'network.host: localhost' >>$cfg1
echo 'network.host: localhost' >>$cfg2

echo 'http.port: 9200' >>$cfg1
echo 'http.port: 9205' >>$cfg2


