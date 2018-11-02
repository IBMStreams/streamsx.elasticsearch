# Using the Elasticsearch server for manual testing

## Prerequisites
* Install a JAVA 1.8 runtime, the patch level must be greater than 131
* Set the environment variable ELA_JAVA_HOME to the Java installation you want to use. If your default Java is new enough, this step can be omitted. The installation pointed to in ELA_JAVA_HOME is only used by the runNode scripts to start the ES server
* Set the environment variable ELA_VERSION to the desired Elasticsearch server version. If unset, the default version is 6.2.2. If the server software is not already present in this directory, it is downloaded from artifacts.elastic.co

## Install and start the server/cluster 
* Install the server. This will download the software and create directories for two nodes
```	
  ./installServer.sh
```
* Start the first node (http port 9200)
```
  ./runNode.sh 1
```
* Optionally start the second node (http port 9205) in a new terminal
```
  ./runNode.sh 2
```
* Check the cluster health
```
  curl -X GET "localhost:9200/_cat/health?v"
```
## Run your manual tests
* Run the test application (maybe a sample from the toolkit)

## Verify results
To verify results, use the following commands to check documents and indices

List all indices
	
	curl -X GET "localhost:9200/_cat/indices?v"
	
List the first 10 documents in the index 'ecg_index' 
	
	curl -X GET "localhost:9200/ecg_index/_search?q=*&pretty"
	
Delete an index named 'basic_test_index'
	
	curl -X DELETE 'http://localhost:9200/basic_test_index'

## Stop the server
Just press CRTL-C in the terminal windows, where you started the nodes.
To remove the server directories, you can use the cleanServer.sh script

	./cleanServer.sh

