# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import datetime

import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring

    
def bulk_insert(stream, indexName, bulkSize=1, messageAttribute=None, credentials='es', vmArg=None, name=None):
    """Stores JSON documents in a specified index of an Elasticsearch database.

    Ingests tuples and stores them in Elasticsearch as documents when bulk size is reached.
    If input is ``streamsx.topology.schema.StreamSchema``, then each attribute in the input schema will become an document attribute, the name of the JSON attribute will be the name of the Streams tuple attribute, the value will be taken from the attributes value. 
    Writes JSON documents without conversion, when input stream is ``CommonSchema.Json``.

    Args:
        stream(Stream): Stream of tuples stored in Elasticsearch as documents. Supports ``CommonSchema.Json`` in the input stream to store the JSON messages in Elasticsearch. Otherwise each attribute in the input schema will become an document attribute, the name of the JSON attribute will be the name of the Streams tuple attribute, the value will be taken from the attributes value.
        indexName(str): Name of the Elasticsearch index, the documents will be inserted to. If the index does not exist in the Elasticsearch server, it will be created by the server. However, you should create and configure indices by yourself before using them, to avoid automatic creation with properties that do not match the use case. For example unsuitable mapping or number of shards or replicas. 
        bulkSize(int): Size of the bulk to submit to Elasticsearch. The default value is 1.      
        messageAttribute(str): Name of the input stream attribute containing the JSON document. Parameter is not required when input stream schema is ``CommonSchema.Json``.                     
        credentials(str): Name of the application configuration containing the credentials for Elasticsearch database. When not set, the application configuration name ``es`` is used.
        vmArg(str): Arbitrary JVM arguments can be passed using vmArg.
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Sink: Stream termination.
    """
    if stream.oport.schema == CommonSchema.Json:
        messageAttribute = 'jsonString'     

    _op = _ElasticsearchIndex(stream, indexName=indexName, bulkSize=bulkSize, appConfigName=credentials, vmArg=vmArg, name=name)
    if messageAttribute is not None:
       _op.params['documentAttribute'] = _op.attribute(stream, messageAttribute)

    return streamsx.topology.topology.Sink(_op)


class _ElasticsearchIndex(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, vmArg=None, appConfigName=None, bulkSize=None, connectionTimeout=None, documentAttribute=None, hostName=None, hostPort=None, idName=None, idNameAttribute=None, indexName=None, indexNameAttribute=None, maxConnectionIdleTime=None, nodeList=None, password=None, readTimeout=None, reconnectionPolicyCount=None, sslDebug=None, sslEnabled=None, sslTrustAllCertificates=None, sslTrustStore=None, sslTrustStorePassword=None, sslVerifyHostname=None, storeTimestamps=None, timestampName=None, timestampValueAttribute=None, userName=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.elasticsearch::ElasticsearchIndex"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if bulkSize is not None:
            params['bulkSize'] = bulkSize
        if connectionTimeout is not None:
            params['connectionTimeout'] = connectionTimeout
        if documentAttribute is not None:
            params['documentAttribute'] = documentAttribute
        if hostName is not None:
            params['hostName'] = hostName
        if hostPort is not None:
            params['hostPort'] = hostPort
        if idName is not None:
            params['idName'] = idName
        if idNameAttribute is not None:
            params['idNameAttribute'] = idNameAttribute
        if indexName is not None:
            params['indexName'] = indexName
        if indexNameAttribute is not None:
            params['indexNameAttribute'] = indexNameAttribute
        if maxConnectionIdleTime is not None:
            params['maxConnectionIdleTime'] = maxConnectionIdleTime
        if nodeList is not None:
            params['nodeList'] = nodeList
        if password is not None:
            params['password'] = password
        if readTimeout is not None:
            params['readTimeout'] = readTimeout
        if reconnectionPolicyCount is not None:
            params['reconnectionPolicyCount'] = reconnectionPolicyCount
        if sslDebug is not None:
            params['sslDebug'] = sslDebug
        if sslEnabled is not None:
            params['sslEnabled'] = sslEnabled
        if sslTrustAllCertificates is not None:
            params['sslTrustAllCertificates'] = sslTrustAllCertificates
        if sslTrustStore is not None:
            params['sslTrustStore'] = sslTrustStore
        if sslTrustStorePassword is not None:
            params['sslTrustStorePassword'] = sslTrustStorePassword
        if sslVerifyHostname is not None:
            params['sslVerifyHostname'] = sslVerifyHostname
        if storeTimestamps is not None:
            params['storeTimestamps'] = storeTimestamps
        if timestampName is not None:
            params['timestampName'] = timestampName
        if timestampValueAttribute is not None:
            params['timestampValueAttribute'] = timestampValueAttribute
        if userName is not None:
            params['userName'] = userName

        super(_ElasticsearchIndex, self).__init__(topology,kind,inputs,schema,params,name)



