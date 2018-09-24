HOW TO RUN THE TESTS
-----------------------------------

Prerequisites
1) install a JAVA 1.8 environment, patchlevel must be greater than 131
2) adapt environment.txt : set JAVA_HOME , set the elasticsearchversion to download
3) source the environment:
  . environment.txt

Install and start the cluster 
4) install the server:
   ./installServer.sh
5) to start the first node (http port 9200), enter:
   ./runNode.sh 1
6) optionally start the second node (http port 9205) in a new terminal :
   ./runNode.sh 2
7) check cluster health :
   curl -X GET "localhost:9200/_cat/health?v"

Start the test (TBD)
8) run the sample application

Verify results, check documents in index 
9) run :
   curl -X GET "localhost:9200/_cat/indices?v"
   curl -X GET "localhost:9200/ecg_index/_search?q=*&pretty"


