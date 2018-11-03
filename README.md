# streamsx.elasticsearch

Toolkit for sending tuple data from Streams applications to Elasticsearch.

- `ElasticsearchIndex` operator: Uses [Jest](https://github.com/searchbox-io/Jest), a Java HTTP Rest client, to send data to Elasticsearch.

## Documentation

Find the SPLDOC documentation [here](https://ibmstreams.github.io/streamsx.elasticsearch/doc/spldoc/html/).

## IBM Cloud

This toolkit is compatible with the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service and [Compose for Elasticsearch](https://www.ibm.com/cloud/compose/elasticsearch) service on IBM Cloud.

### Get Started

1.  Clone the repository.
2.  Run `ant all` to build toolkit.
3.  Run `ant build-all-samples` to build the samples.

### To develop in Studio:

1.  Clone the repository.
2.  `cd com.ibm.streamsx.elasticsearch`
3.  Run `ant maven-deps` to download the dependencies required by the project.
3.  In Streams Studio, Import...
5.  In the dialog, select IBM Streams -> SPL Project.
6.  Select com.ibm.streamsx.elasticsearch to import the project into Studio.

### Testing

To test the toolkit and setup a server for testing see the following pages
* [Local Server Setup](https://github.com/IBMStreams/streamsx.elasticsearch/blob/develop/tests/setup/README.md)
* [Run the testsuite using the local server](https://github.com/IBMStreams/streamsx.elasticsearch/blob/develop/tests/fwtests/README.md)
* [Run the Cloud tests using Streams analytics and a Elasticsearch Compose service](https://github.com/IBMStreams/streamsx.elasticsearch/blob/develop/tests/cloud/README.md)

### Install Elasticsearch

Refer to official [Download and Installation Steps](https://www.elastic.co/downloads/elasticsearch)
or create a [Compose for Elasticsearch](https://console.bluemix.net/docs/services/ComposeForElasticsearch/index.html) IBM Cloud service.

