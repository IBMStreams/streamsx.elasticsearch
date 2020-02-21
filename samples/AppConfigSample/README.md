## ElasticsearchAppConfigSample

In this sample SPL application the ElasticsearchIndex operator requires the connection settings given in an  application configuration.
Either connect to the IBM cloud service [Compose for Elasticsearch](https://console.bluemix.net/docs/services/ComposeForElasticsearch/index.html) or a local installation.

### Use

Build the applications:

`make`

### Create application configuration

When running in local Streams installation, you can create the application configuration `es` with the following command:

`make configure`

This command uses the `streamtool mkappconfig` command and requires the environment variables:

* ES_USER
* ES_PASSWORD
* ES_NODES

When launching in Streaming Analytics service, you need to create the application configuration `es` in the Streams Console.

### Launch the application

You need to launch the application in distributed mode, otherwise the application configuration is not read and the parameter default are used instead.

`st submitjob output/com.ibm.streamsx.elasticsearch.sample.AppConfig.Main.sab -P indexName=index2`


### Result

Verify that data is written into `index2` with the connection string of your instance:

curl -k -u admin:PASSWORD 'https://portal-ssl11-22.bmix-eu-gb-yp-0a000dc-a22d-22bd-a333-4444c555ba66.77777777.composedb.com:15149/index2/_search?q=*&pretty'


### Clean:

`make clean`

