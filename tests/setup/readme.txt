HOW TO TEST MANUALLY
-----------------------------------

Prerequisites
1) install a JAVA 1.8 environment, patchlevel must be greater than 131
2) set the ENV variable ELA_JAVA_HOME to the Java installation you want to use
   if your default Java is new enough, this step can be omitted. 
   The installation pointed to in ELA_JAVA_HOME is only used by the runNode scripts to start the ES server
3) set the ENV var ELA_VERSION to the desired Elasticsearch server version. If unset, the default version
   is 6.2.2. If the server software is not already present in this directory,
   it is downloaded from artifacts.elastic.co

Install and start the server/cluster 
4) install the server. This will download the software and create directories for two nodes
   ./installServer.sh
5) to start the first node (http port 9200), enter:
   ./runNode.sh 1
6) optionally start the second node (http port 9205) in a new terminal :
   ./runNode.sh 2
7) check cluster health :
   curl -X GET "localhost:9200/_cat/health?v"

Run your test
8) run the test application (maybe a sample)

To verify results, check documents in index, for example like this:
9) run :
   curl -X GET "localhost:9200/_cat/indices?v"
   curl -X GET "localhost:9200/ecg_index/_search?q=*&pretty"


