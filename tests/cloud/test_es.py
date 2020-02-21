import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import streamsx.rest as sr
import os, os.path
import subprocess
from subprocess import call, Popen, PIPE
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import urllib3

import streamsx.topology.context
import requests
from urllib.parse import urlparse

def streams_install_env_var():
    result = True
    try:
        os.environ['STREAMS_INSTALL']
    except KeyError: 
        result = False
    return result

class Test(unittest.TestCase):
    """ Test invocations of composite operators in local Streams instance """

    @classmethod
    def setUpClass(self):
        print (str(self))
        print ("Setup Elasticsearch client ...")
        # ES client expects ES_URL environment variable with URL to Compose Elasticsearch service, e.g. https://user:password@portalxxx.composedb.com:port/
        es_url = os.environ['ES_URL']
        print (str(es_url))
        self._es = Elasticsearch([es_url],verify_certs=False)
        self._indexName = 'test-index-cloud'
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        creds = urlparse(es_url)
        self._es_user_name = creds.username
        self._es_password = creds.password
        self._es_node_list = creds.hostname+':'+str(creds.port)


    def setUp(self):
        Tester.setup_distributed(self)
        self.elasticsearch_toolkit_location = "../../com.ibm.streamsx.elasticsearch"

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

        # change trace level
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)

        if ("TestCloud" not in str(self)):
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

    def _run_shell_command_line(self, command):
        process = Popen(command, universal_newlines=True, shell=True, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        return stdout, stderr, process.returncode

    def _create_app_config(self):
        if ("TestICP" in str(self) or "TestCloud" in str(self) ):
            print ("Ensure that application configuration 'es' is created.")
        else:
            if streams_install_env_var():
                print ("Create elasticsearch application configuration with streamtool")
                this_dir = os.path.dirname(os.path.realpath(__file__))
                app_dir = this_dir+'/es_test'
                stdout, stderr, err = self._run_shell_command_line('export ES_NODES='+self._es_node_list+';'+'export ES_USER='+self._es_user_name+';'+'export ES_PASSWORD='+self._es_password+';'+'cd '+app_dir+'; make configure')
                print (str(err))

    # ------------------------------------

    # CONSISTENT REGION test with TopologyTester:
    # Resets triggered by ConsistentRegionResetter and Beacon re-submits the tuples
    def test_consistent_region_with_resets(self):
        self._indexName = 'test-index-cr'
        self._create_app_config()
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
        self._create_app_config()
        # delete index before launching Streams job
        self._es.indices.delete(index=self._indexName, ignore=[400, 404]) 
        numTuples = 20000 # num generated tuples
        bulkSize = 1000
        self._build_launch_app("test_bulk", "com.ibm.streamsx.elasticsearch.test::TestBulk", {'indexName':self._indexName, 'numTuples':numTuples, 'bulkSize':bulkSize}, numTuples, 'es_test')
        self._validate_count(self._indexName, numTuples);

    # ------------------------------------


class TestLocal(Test):
    """ Test invocations of composite operators in local Streams instance using installed toolkit """

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.elasticsearch_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.elasticsearch'


class TestICP(Test):
    """ Test in ICP env using local toolkit (repo) """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        env_chk = True
        try:
            print("CP4D_URL="+str(os.environ['CP4D_URL']))
        except KeyError:
            env_chk = False
        assert env_chk, "CP4D_URL environment variable must be set"


class TestICPLocal(TestICP):
    """ Test in ICP env using local installed toolkit (STREAMS_INSTALL/toolkits) """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.elasticsearch_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.elasticsearch'


class TestICPRemote(TestICP):
    """ Test in ICP env using remote toolkit (build service) """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.elasticsearch_toolkit_location = None


class TestCloud(Test):
    """ Test in Streaming Analytics Service using local toolkit (repo) """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.elasticsearch_toolkit_location = "../../com.ibm.streamsx.elasticsearch"


class TestCloudLocal(TestCloud):
    """ Test in Streaming Analytics Service using local installed toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.elasticsearch_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.elasticsearch'


class TestCloudLocalRemote(TestCloud):
    """ Test in Streaming Analytics Service using local installed toolkit and remote build """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.elasticsearch_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.elasticsearch'


class TestCloudRemote(Test):
    """ Test in Streaming Analytics Service using remote toolkit and remote build """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        # remote toolkit is used
        self.elasticsearch_toolkit_location = None


