---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2018-11-20T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}


# Develop Streams Elasticsearch toolkit

## Downloading the Streams Elasticsearch toolkit

Download the full toolkit requires git. Enter a directory of your choice and execute :

`cd yourDirectory`

`git clone https://github.com/IBMStreams/streamsx.elasticsearch.git`

## Build the toolkit using ant

These steps must be run on the Streams server (or the Streams Quick Start Edition) and require the Ant and Maven tools. Additionally, environment variable M2_HOME must be set to the Maven home directory.
* Set the Streams environment variables by sourcing the `streamsprofile.sh` script which can be found in the Streams installation `bin` directory
* Set environment variable `M2_HOME` to the Maven home directory; if you build the toolkit on a Streams QSE image, Maven is already installed and M2_HOME set
* Go to the toolkit's main directory that holds the `build.xml` file, for example: `cd /yourDirectory/streamsx.elasticsearch`
* Run `ant`
* To check out more targets of the build script run: `ant -p`

## Test the toolkit

To run the complete test suite, execute:

`ant test`

For more information read the file [TEST](tests/fwtests/README.md) .

## Clean the toolkit

To delete generated files, execute:

`ant clean`

