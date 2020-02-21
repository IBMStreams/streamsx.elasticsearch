## ElasticsearchSSLCloudSample

This sample SPL application demonstrates the use of the ElasticsearchIndex operator with SSL connection to the IBM cloud service [Compose for Elasticsearch](https://console.bluemix.net/docs/services/ComposeForElasticsearch/index.html).


### Use

Build the applications:

`make`

### Launch the application

When launching the application in standalone mode, you need to update `nodeList`, `user` and `password` parameters for your Elasticsearch service.

`./output/bin/standalone userName=admin password=PASSWORD nodeList=portal-ssl11-22.bmix-eu-gb-yp-0a000dc-a22d-22bd-a333-4444c555ba66.77777777.composedb.com:15149`


### Result

Verify that data is written into `index1` with the connection string of your instance:

curl -k -u admin:PASSWORD 'https://portal-ssl11-22.bmix-eu-gb-yp-0a000dc-a22d-22bd-a333-4444c555ba66.77777777.composedb.com:15149/index1/_search?q=*&pretty'


### Clean:

`make clean`

