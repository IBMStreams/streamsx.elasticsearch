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
        self._indexName = 'test-index-cloud'

    def setUp(self):
        Tester.setup_distributed(self)
        self.elasticsearch_toolkit_location = "../../com.ibm.streamsx.elasticsearch"
        self.isCloudTest = False

    def tearDown(self):
        self._es.indices.delete(index=self._indexName, ignore=[400, 404])

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
        job_config = streamsx.topology.context.JobConfig(tracing='warn')
        # icp config
        if ("TestICP" in str(self)):
            job_config.raw_overlay = {"configInstructions": {"convertTagSet": [ {"targetTagSet":["python"] } ]}}
            cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        job_config.add(cfg)

        if ("TestDistributed" in str(self)):
            cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        test_res = self.tester.test(self.test_ctxtype, cfg, assert_on_fail=True, always_collect_logs=True)
        print (str(self.tester.result))        
        assert test_res, name+" FAILED ("+self.tester.result["application_logs"]+")"

    def _validate_count(self, indexName, expectedNum):
        # check the count
        count = self._es.count(index=indexName, doc_type='_doc', body={ "query": {"match_all" : { }}})
        print("Count: "+str(count['count']))
        assert (count['count'] == expectedNum), "Wrong tuple count (expected="+str(expectedNum)+"): "+str(count['count'])        

    # ------------------------------------

    # CONSISTENT REGION test with TopologyTester:
    # Resets triggered by ConsistentRegionResetter and Beacon re-submits the tuples
    def test_consistent_region_with_resets(self):
        self._indexName = 'test-index-cr'
        # delete index before launching Streams job
        self._es.indices.delete(index=self._indexName, ignore=[400, 404]) 
        numResets = 3
        runFor = 150
        numTuples = 300 # num generated tuples
        drainPeriod = 5.0
        self._build_launch_app("test_consistent_region_with_resets", "com.ibm.streamsx.elasticsearch.test::TestConsistentRegionAppConfig", {'indexName':self._indexName, 'drainPeriod':drainPeriod, 'numTuples':numTuples}, numTuples, 'es_test', False, runFor, numResets)
        self._validate_count(self._indexName, numTuples);
  
    # ------------------------------------

    def test_bulk(self):
        self._indexName = 'test-index-bulk'
        # delete index before launching Streams job
        self._es.indices.delete(index=self._indexName, ignore=[400, 404]) 
        numTuples = 20000 # num generated tuples
        bulkSize = 1000
        self._build_launch_app("test_bulk", "com.ibm.streamsx.elasticsearch.test::TestBulk", {'indexName':self._indexName, 'numTuples':numTuples, 'bulkSize':bulkSize}, numTuples, 'es_test')
        self._validate_count(self._indexName, numTuples);

    # ------------------------------------

class TestInstall(TestDistributed):
    """ Test invocations of composite operators in local Streams instance using installed toolkit """

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.elasticsearch_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.elasticsearch'


class TestICP(TestDistributed):
    """ Test invocations of composite operators in remote Streams instance using local toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        env_chk = True
        try:
            print("STREAMS_REST_URL="+str(os.environ['STREAMS_REST_URL']))
        except KeyError:
            env_chk = False
        assert env_chk, "STREAMS_REST_URL environment variable must be set"


class TestICPInstall(TestICP):
    """ Test invocations of composite operators in remote Streams instance using local installed toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

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


