import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import os, os.path
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class TestDistributed(unittest.TestCase):
    """ Test invocations of composite operators in local Streams instance """

    @classmethod
    def setUpClass(self):
        print (str(self))
        print ("Setup Elasticsearch client ...")
        # ES client expects ES_URL environment variable with URL to Compose Elasticsearch service, e.g. https://user:password@portalxxx.composedb.com:port/
        es_url = os.environ['ES_URL']
        self._es = Elasticsearch([es_url],verify_certs=True)

    def setUp(self):
        Tester.setup_distributed(self)
        self.elasticsearch_toolkit_location = "../../com.ibm.streamsx.elasticsearch"
        self.isCloudTest = False

    def _add_toolkits(self, topo, test_toolkit):
        tk.add_toolkit(topo, test_toolkit)
        if self.elasticsearch_toolkit_location is not None:
            tk.add_toolkit(topo, self.elasticsearch_toolkit_location)

    def _build_launch_app(self, name, composite_name, parameters, num_result_tuples, test_toolkit, exact=True, run_for=60, resets=0):
        print ("------ "+name+" ------")
        topo = Topology(name)
        self._add_toolkits(topo, test_toolkit)
	
        params = parameters
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)
        self.tester = Tester(topo)
        self.tester.run_for(run_for)
        if (resets > 0):
            self.tester.resets(resets) # minimum number of resets for each region, requires v1.11 of topology toolkit
        self.tester.tuple_count(test_op.stream, num_result_tuples, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='debug')
        job_config.add(cfg)

        # Run the test
        test_res = self.tester.test(self.test_ctxtype, cfg, assert_on_fail=True, always_collect_logs=True)
        print (str(self.tester.result))        
        assert test_res, name+" FAILED ("+self.tester.result["application_logs"]+")"

    def _validate_count(self, indexName, expectedNum):
        # check the count
        count = self._es.count(index=indexName, doc_type='_doc', body={ "query": {"match_all" : { }}})
        print("Count: "+str(count['count']))
        assert (count['count'] >= expectedNum), "Missing tuples (expected="+str(expectedNum)+"): "+str(count['count'])        

    # ------------------------------------

    # CONSISTENT REGION test with TopologyTester:
    # Resets triggered by ConsistentRegionResetter and Beacon re-submits the tuples
    def test_consistent_region_with_resets(self):
        indexName = 'test-index-cr'
        # delete index before launching Streams job
        self._es.indices.delete(index=indexName, ignore=[400, 404]) 
        numResets = 3
        runFor = 150
        numTuples = 300 # num generated tuples
        drainPeriod = 5.0
        self._build_launch_app("test_consistent_region_with_resets", "com.ibm.streamsx.elasticsearch.test::TestConsistentRegionAppConfig", {'indexName':indexName, 'drainPeriod':drainPeriod, 'numTuples':numTuples}, numTuples, 'es_test', False, runFor, numResets)
        self._validate_count(indexName, numTuples);
  
    # ------------------------------------

    def test_bulk(self):
        indexName = 'test-index-bulk'
        # delete index before launching Streams job
        self._es.indices.delete(index=indexName, ignore=[400, 404]) 
        numTuples = 20000 # num generated tuples
        bulkSize = 1000
        self._build_launch_app("test_consistent_region_with_resets", "com.ibm.streamsx.elasticsearch.test::TestBulk", {'indexName':indexName, 'numTuples':numTuples, 'bulkSize':bulkSize}, numTuples, 'es_test')
        self._validate_count(indexName, numTuples);

    # ------------------------------------

class TestInstall(TestDistributed):
    """ Test invocations of composite operators in local Streams instance using installed toolkit """

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.elasticsearch_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.elasticsearch'


class TestCloud(TestDistributed):
    """ Test invocations of composite operators in Streaming Analytics Service using local toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.elasticsearch_toolkit_location = "../../com.ibm.streamsx.elasticsearch"
        self.isCloudTest = True
        

class TestCloudInstall(TestDistributed):
    """ Test invocations of composite operators in Streaming Analytics Service using remote toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        # remote toolkit is used
        self.elasticsearch_toolkit_location = None
        self.isCloudTest = True


