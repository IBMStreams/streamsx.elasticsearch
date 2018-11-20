---
title: "Toolkit technical background overview"
permalink: /docs/knowledge/overview/
excerpt: "Basic knowledge of the toolkits technical domain."
last_modified_at: 2018-11-20T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "knowledgedocs"
---
{% include toc %}
{% include editme %}

The Elasticsearch toolkit is an open source IBM Streams toolkit that provides adapters for the 
[Elasticsearch](https://www.elastic.co/products/elasticsearch) search and analytics engine.

It provides the operators
* `ElasticsearchIndex` to store tuple data as JSON documents in Elasticsearch indices

The toolkit supports the following features
* Streams application configuration
* Connecting to multiple Elasticsearch nodes in a cluster
* Consistent region features to achieve at-least-once semantic where appropriate
* Using SSL/TLS with HTTP Basic authentication to connect to servers

This version of the toolkit is intended for use with IBM Streams release 4.2 and later.
