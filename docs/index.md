---
layout: default
---

## [](#header-1)What's New?

*   **Store and Graph Data with the Elasticsearch Toolkit [Coming Soon]**
*   **[SPLDoc for the Elasticsearch toolkit](https://ibmstreams.github.io/streamsx.elasticsearch/doc/spldoc/html/index.html)**

## [](#header-2)Prerequisites

1.  **IBM Streams** -- If you don't have this installed, you can try it by downloading the [Quick Start Edition](http://ibm.co/streamsqs).
2.  **streamsx.elasticsearch** -- Download the toolkit, either by cloning it from the official [repository](https://github.com/IBMStreams/streamsx.elasticsearch) or using the download links above. See _Setup Instructions_ below.

## [](#header-3)Setup Instructions

1.	Clone and build the **streamsx.elasticsearch** toolkit by running the following commands:
	```bash
	git clone https://github.com/IBMStreams/streamsx.elasticsearch.git
	cd streamsx.elasticsearch/com.ibm.streamsx.elasticsearch
	ant all
	```
2.  Add the toolkit as a dependency to your Streams application.

_Note: If you don’t have a Streams application to test with, there are samples in the toolkit’s `samples` folder you may reference._

## [](#header-4)Configuration

### Document storage parameters

| Parameter               		| Type          | Description      |
|:----------------------------- |:------------- |:---------------- |
| **indexName**			        | _rstring_     | Specifies the name of the index. |
| **typeName**   				| _rstring_     | Specifies the name of the type. |
| **idName**  					| _rstring_     | Specifies the name of the id.  If not specified, id is auto-generated. |
| **storeTimestamps**			| _boolean_		| Enables storing timestamps. |
| **timestampName** 			| _rstring_     | Specifies the name of the timestamp (default: timestamp). |

### Document storage parameters (dynamic)

| Parameter               		| Type          | Description      |
|:----------------------------- |:------------- |:---------------- |
| **indexAttribute**			| _Attribute_   | Specifies the attribute providing the index names. |
| **typeAttribute**    			| _Attribute_   | Specifies the attribute providing the type names. |
| **idAttribute**  				| _Attribute_   | Specifies the attribute providing the id names. |
| **timestampValueAttribute** 	| _Attribute_   | Specifies the attribute providing the timestamp values (default: current time data is indexed). |

### Other configurations

| Parameter               		| Type          | Description      |
|:----------------------------- |:------------- |:---------------- |
| **hostName** 					| _rstring_     | Specifies the hostname of the Elasticsearch server (default: localhost). |
| **hostPort**  				| _int32_     	| Specifies the hostport of the Elasticsearch server (default: 9300). |
| **bulkSize**  				| _int32_     	| Specifies the size of the bulk to submit to Elasticsearch. |
| **reconnectionPolicyCount** 	| _int32_     	| Specifies the number of times to attempt reconnection to the Elasticsearch server, upon disconnection. |
| **sizeMetricsEnabled**		| _boolean_		| Specifies whether to store and aggregate size metrics. |

## [](#header-6)Other Links

*   [IBM Streams on Github](http://ibmstreams.github.io)
*   [Introduction to Streams Quick Start Edition](http://ibmstreams.github.io/streamsx.documentation/docs/4.2/qse-intro/)
*   [Streams Getting Started Guide](http://ibmstreams.github.io/streamsx.documentation/docs/4.2/qse-getting-started/)
*   [StreamsDev](https://developer.ibm.com/streamsdev/)