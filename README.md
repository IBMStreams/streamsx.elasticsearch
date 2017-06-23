# streamsx.elasticsearch

Toolkit for sending tuple data from a Streams application to Elasticsearch

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
