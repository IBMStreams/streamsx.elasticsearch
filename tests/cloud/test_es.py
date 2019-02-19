import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import streamsx.rest as sr
import os, os.path
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import urllib3

import streamsx.topology.context
import requests
from urllib.parse import urlparse

class Test(unittest.TestCase):
    """ Test invocations of composite operators in local Streams instance """

    @classmethod
    def setUpClass(self):
        print (str(self))
        print ("Setup Elasticsearch client ...")
        # ES client expects ES_URL environment variable with URL to Compose Elasticsearch service, e.g. https://user:password@portalxxx.composedb.com:port/
        es_url = os.environ['ES_URL']
        self._es = Elasticsearch([es_url],verify_certs=False)
        self._indexName = 'test-index-cloud'
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 

    def setUp(self):
        Tester.setup_distributed(self)
        self.elasticsearch_toolkit_location = "../../com.ibm.streamsx.elasticsearch"

    def tearDown(self):
        self._es.indices.delete(index=self._indexName, ignore=[400, 404])

    def _add_toolkits(self, topo, test_toolkit):
        tk.add_toolkit(topo, test_toolkit)
        if self.elasticsearch_toolkit_location is not None:
            tk.add_toolkit(topo, self.elasticsearch_toolkit_location)

    def _service (self, force_remote_build = True):
        auth_host = os.environ['AUTH_HOST']
        auth_user = os.environ['AUTH_USERNAME']
        auth_password = os.environ['AUTH_PASSWORD']
        streams_rest_url = os.environ['STREAMS_REST_URL']
        streams_service_name = os.environ['STREAMS_SERVICE_NAME']
        streams_build_service_port = os.environ['STREAMS_BUILD_SERVICE_PORT']
        uri_parsed = urlparse (streams_rest_url)
        streams_build_service = uri_parsed.hostname + ':' + streams_build_service_port
        streams_rest_service = uri_parsed.netloc
        r = requests.get ('https://' + auth_host + '/v1/preauth/validateAuth', auth=(auth_user, auth_password), verify=False)
        token = r.json()['accessToken']
        cfg = {
            'type': 'streams',
            'connection_info': {
                'serviceBuildEndpoint': 'https://' + streams_build_service,
                'serviceRestEndpoint': 'https://' + streams_rest_service + '/streams/rest/instances/' + streams_service_name
            },
            'service_token': token
        }
        cfg [streamsx.topology.context.ConfigParams.FORCE_REMOTE_BUILD] = force_remote_build
        return cfg

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
        if ("TestICP" in str(self)):
            remote_build = False
            if ("TestICPRemote" in str(self)):
                remote_build = True
            cfg = self._service(remote_build)

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
            print("STREAMS_REST_URL="+str(os.environ['STREAMS_REST_URL']))
        except KeyError:
            env_chk = False
        assert env_chk, "STREAMS_REST_URL environment variable must be set"


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


