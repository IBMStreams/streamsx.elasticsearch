from unittest import TestCase

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.elasticsearch as es
import streamsx.spl.toolkit
import os
import uuid

##
## Test assumptions
##
## Streaming analytics service has:
##    application config 'es' configured for Elasticsearch database
##

class JsonData(object):
    def __init__(self, prefix, count):
        self.prefix = prefix
        self.count = count
    def __call__(self):
        for i in range(self.count):
            yield {'p': self.prefix + '_' + str(i), 'c': i}

class StringData(object):
    def __init__(self, prefix, count):
        self.prefix = prefix
        self.count = count
    def __call__(self):
        for i in range(self.count):
            yield self.prefix + '_' + str(i)
        

class TestES(TestCase):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.es_toolkit_home = os.environ["ELASTICSEARCH_TOOLKIT_HOME"]

    def test_json(self):
        n = 100
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, self.es_toolkit_home)

        uid = str(uuid.uuid4())
        s = topo.source(JsonData(uid, n)).as_json()
        es.bulk_insert(s, 'test-index-cloud', 10)

        tester = Tester(topo)
        tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    def test_string(self):
        n = 100
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, self.es_toolkit_home)

        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        es.bulk_insert(s, 'test-index-cloud', 10)

        tester = Tester(topo)
        tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)
 
    def test_hw(self):
        n = 100
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, self.es_toolkit_home)

        s = topo.source(['Hello', 'World!']).as_string()
        es.bulk_insert(s, 'test-index-cloud')

        tester = Tester(topo)
        tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

