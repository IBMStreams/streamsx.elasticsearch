# streamsx.elasticsearch tests

## Run test

Per default the Elasticsearch server 6.2.2 is used for testing. This server requires JAVA version 1.8 patchlevel greater than 131.
If you want to use a different JAVA installation for starting the server, than your default installation, set the environment variable ELA_JAVA_HOME to the point to a suitable installation.
For example like:

	export ELA_JAVA_HOME=/usr/lib/jvm/jre-1.8.0-ibm.x86_64
 
### Run all test suites with local Streams instance

    ./runTest.sh -s

### Run a single test case

Example for running a single test case: 

    ./runTest.sh -s --noprompt --no-browser "*::CompileErrorTest"




