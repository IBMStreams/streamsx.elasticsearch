# streamsx.elasticsearch

Toolkit for sending tuple data from Streams applications to Elasticsearch. Contains 2 operators:

- `ElasticsearchIndex`: Uses the Elasticsearch Java API's [Transport Client](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html) to send data to Elasticsearch.
- `ElasticsearchRestIndex`: Uses [Jest](https://github.com/searchbox-io/Jest), a Java HTTP Rest client, to send data to Elasticsearch.

Both operators perform the same functions - just through different clients/methods. Read more about the different clients [here](https://www.elastic.co/blog/found-interfacing-elasticsearch-picking-client).

### Get Started

1.  Clone the repository.
2.  `cd com.ibm.streamsx.elasticsearch`
3.  Run `ant all` to build toolkit.

### To develop in Studio:

1.  Clone the repository.
2.  `cd com.ibm.streamsx.elasticsearch`
3.  Run `ant maven-deps` to download the dependencies required by the project.
3.  In Streams Studio, Import...
5.  In the dialog, select IBM Streams -> SPL Project.
6.  Select com.ibm.streamsx.elasticsearch to import the project into Studio.

### Install Elasticsearch

Refer to official [Download and Installation Steps](https://www.elastic.co/downloads/elasticsearch).

#### Install Elasticsearch Mapper Size Plugin
The mapper size plugin gives the ElasticsearchSink the ability to query the size of documents indexed. Some metrics will only display once this plugin is installed.

### SPLDOC documentation

You can generate HTML from the SPLDOC with the following commands:

```
spl-make-toolkit -i com.ibm.streamsx.elasticsearch
spl-make-doc -i com.ibm.streamsx.elasticsearch
```

You can view the HTML pages with any browser, or, for example, the following
command:

```
firefox com.ibm.streamsx.elasticsearch/doc/spldoc/html/index.html &
```
