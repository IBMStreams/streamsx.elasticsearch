//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch;

import java.io.IOException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

@PrimitiveOperator(
		name="ElasticsearchIndex",
		namespace="com.ibm.streamsx.elasticsearch",
		description=ElasticsearchIndex.DESC_OPERATOR
		)
@InputPorts({
	@InputPortSet(
			description="Port that ingests tuples",
			cardinality=1,
			optional=false,
			windowingMode=WindowMode.NonWindowed,
			windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious
			)
})
@Libraries({
	"opt/downloaded/*"
	})
public class ElasticsearchIndex extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------

	static final String DESC_OPERATOR = 
			"The ElasticsearchIndex operator receives incoming tuples and outputs "
			+ "the attribute's name-value pairs to an Elasticsearch database.\\n"
			+ "\\n"
			+ "The ElasticsearchIndex requires a hostname and hostport of an "
			+ "Elasticsearch server to connect to.\\n"
			+ "\\n"
			+ "By default, the hostname is 'localhost', and the hostport "
			+ "is 9300. This configuration can be changed in the parameters.\\n"
			+ "\\n"
			+ "An index and type must also be specified. The id is optional and if"
			+ "not specified, it is auto-generated.\\n"
			+ "\\n"
			+ "A timestampName can optionally be specified for time-based queries "
			+ "to create time-series charts that display how a tuple's attribute value "
			+ "changes over time.\\n"
			+ "\\n"
			+ "Once the data is outputted to Elasticsearch, the user can query the "
			+ "database and create custom graphs to display this data with graphing "
			+ "tools such as Grafana and Kibana.\\n"
			+ "\\n"
			;
	
	@Parameter(
			optional=true,
			description="Specifies the hostname of the Elasticsearch server (default: localhost)."
			)
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	@Parameter(
			optional=true,
			description="Specifies the hostport of the Elasticsearch server (default: 9300)."
			)
	public void setHostPort(int hostPort) {
		this.hostPort = hostPort;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the name for the cluster."
			)
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the name for the index."
			)
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the attribute providing the index names."
			)
	public void setIndexNameAttribute(TupleAttribute<Tuple, String> indexNameAttribute) throws IOException {
		this.indexNameAttribute = indexNameAttribute;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the name for the type."
			)
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the attribute providing the type names."
			)
	public void setTypeNameAttribute(TupleAttribute<Tuple, String> typeNameAttribute) throws IOException {
		this.typeNameAttribute = typeNameAttribute;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the name for the id. If not specified, id is auto-generated."
			)
	public void setIdName(String idName) {
		this.idName = idName;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the attribute providing the ID names."
			)
	public void setIdNameAttribute(TupleAttribute<Tuple, String> idNameAttribute) throws IOException {
		this.idNameAttribute = idNameAttribute;
	}
	
	@Parameter(
			optional=true,
			description="Enables storing timestamps."
			)
	public void setStoreTimestamps(boolean storeTimestamps) {
		this.storeTimestamps = storeTimestamps;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the name for the timestamp attribute."
			)
	public void setTimestampName(String timestampName) {
		this.timestampName = timestampName;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the attribute providing the timestamp values."
			)
	public void setTimestampValueAttribute(TupleAttribute<Tuple, Long> timestampValueAttribute) throws IOException {
		this.timestampValueAttribute = timestampValueAttribute;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the size of the bulk to submit to Elasticsearch."
			)
	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}
	
	@Parameter(
			optional=true,
			description="Specifies the number of times to attempt reconnection to the "
					+ "Elasticsearch server, upon disconnection."
			)
	public void setReconnectionPolicyCount(int reconnectionPolicyCount) {
		this.reconnectionPolicyCount = (long)reconnectionPolicyCount;
	}
	
	@Parameter(
			optional=true,
			description="Specifies whether to store and aggregate size metrics."
			)
	public void setSizeMetricsEnabled(boolean sizeMetricsEnabled) {
		this.sizeMetricsEnabled = sizeMetricsEnabled;
	}
	
	// ------------------------------------------------------------------------
	// Static variables.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(ElasticsearchIndex.class.getName());
	
	/**
	 * Property names for size metrics in Elasticsearch.
	 */
	private static String SIZE_METRICS_PROPERTY = "_size_metrics";
	
	private static String SIZE_PROPERTY = "_size";
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------

	/**
	 * ElasticsearchIndex parameters.
	 */
	private String hostName = "127.0.0.1";
	private int hostPort = 9300;
	
	private String clusterName = "elasticsearch";
	
	private String indexName;
	private TupleAttribute<Tuple, String> indexNameAttribute;

	private String typeName;
	private TupleAttribute<Tuple, String> typeNameAttribute;

	private String idName;
	private TupleAttribute<Tuple, String> idNameAttribute;
	
	private boolean storeTimestamps = false;
	private String timestampName = "timestamp";
	private TupleAttribute<Tuple, Long> timestampValueAttribute;
	
	private int bulkSize = 1;
	
	/**
	 * Elasticsearch client REST API.
	 */
	private TransportClient client;
	private BulkRequestBuilder bulkRequest;
	
	/**
	 * Metrics
	 */
	private Metric isConnected;
	private Metric totalFailedRequests;
	private Metric numInserts;
	private Metric reconnectionCount;
	
	private Metric avgInsertSizeBytes;
	private Metric maxInsertSizeBytes;
	private Metric minInsertSizeBytes;
	private Metric sumInsertSizeBytes;
	
	/**
	 * Metric parameters.
	 */
	private long reconnectionPolicyCount = 1;

	/**
	 * Size metrics should be gathered.
	 */
	private boolean sizeMetricsEnabled = false;
	
	/**
	 * Mapper size plugin is installed.
	 */
	private boolean mapperSizeInstalled = false;
	
	/**
     * Initialize this operator and create Elasticsearch client to send get requests to.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@SuppressWarnings("resource")
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);

    	// Create client that's connected to Elasticsearch server.
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        InetAddress hostAddress = InetAddress.getByName(hostName);
        client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostAddress, hostPort));
	}

	/**
     * Convert incoming tuple attributes to JSON and output them to an Elasticsearch
     * database, configured in the operator's params.
     * Optionally, if 'indexName', 'typeName', and 'idName' attributes are detected 
     * in the tuple's schema, they will be used, instead.
     * will override 
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
    	
    	// Get attribute names.
		StreamSchema schema = tuple.getStreamSchema();
    	Set<String> attributeNames = schema.getAttributeNames();
    	
    	// Add attribute names/values to documentsToAdd.
    	Map<String, Object> documentsToAdd = new HashMap<String, Object>();
    	for(String attributeName : attributeNames) {
    		
    		// Skip attributes used explicitly for defining index, type, and id.
    		if (indexNameAttribute != null && indexNameAttribute.getAttribute().getName().equals(attributeName)) {
    			continue;
    		} else if (typeNameAttribute != null && typeNameAttribute.getAttribute().getName().equals(attributeName)) {
    			continue;
    		} else if (idNameAttribute != null && idNameAttribute.getAttribute().getName().equals(attributeName)) {
    			continue;
    		} else if (timestampValueAttribute != null && timestampValueAttribute.getAttribute().getName().equals(attributeName)) {
    			continue;
    		}
    		
    		if (schema.getAttribute(attributeName).getType().getMetaType() == Type.MetaType.RSTRING) {
    			documentsToAdd.put(attributeName, tuple.getObject(attributeName).toString());
    		} else {
    			documentsToAdd.put(attributeName, tuple.getObject(attributeName));
    		}
    	}

    	// Add timestamp, if it exists.
    	if (storeTimestamps) {
    		DateFormat df = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss.SSSZZ");
    		
    		String timestampToInsert;
    		if (timestampValueAttribute != null) {
    			long timestamp = getTimestampValue(tuple).longValue();
    			timestampToInsert = df.format(new Date(timestamp));
    		} else {
    			timestampToInsert = df.format(new Date(System.currentTimeMillis()));
    		}
    		
    		documentsToAdd.put(timestampName, timestampToInsert);
    	}
    	
        // Convert attributes map to JSON format.
    	XContentBuilder jsonDocuments = XContentFactory.jsonBuilder().map(documentsToAdd);
        
        // Output jsonDocuments to Elasticsearch.
        if (jsonDocuments != null) {
        	
        	// Get index, type, and ID.
        	String indexToInsert = getIndex(tuple);
        	String typeToInsert = getType(tuple);
        	String idToInsert = getId(tuple);
        	
        	if (indexToInsert == null || typeToInsert == null) {
        		throw new Exception("Index and type must be defined.");
        	}
        	
        	if (connectedToElasticsearch(indexToInsert, typeToInsert)) {
	        	
	    		// Add jsonDocuments to bulkRequest.
	        	if (idToInsert != null) {
	        		bulkRequest.add(client.prepareIndex(indexToInsert, typeToInsert, idToInsert)
	        						.setSource(jsonDocuments));
	        	} else {
	        		bulkRequest.add(client.prepareIndex(indexToInsert, typeToInsert)
									.setSource(jsonDocuments));
	        	}
        	
	        	// If bulk size met, output jsonDocuments to Elasticsearch.
	        	if(bulkRequest.numberOfActions() % bulkSize == 0) {
	    			BulkResponse response = bulkRequest.get();
	    			if (!response.hasFailures()) {
	    				long currentNumInserts = numInserts.getValue();
	    				numInserts.setValue(currentNumInserts + bulkSize);
	    			} else {
	    				for (BulkItemResponse item : response) {
	    					if (!item.isFailed()) {
	    						numInserts.increment();
	    					} else {
	    						totalFailedRequests.increment();
	    					}
	    				}
	    			}
	    			
	    			// Get/set size metrics for current type.
	    			if (sizeMetricsEnabled && mapperSizeInstalled) {
	    				getAndSetSizeMetrics(indexToInsert, typeToInsert, idToInsert);
	    			}
	    			
	    			// Clear bulkRequest. Gets recreated in connectedToElasticsearch().
	    			bulkRequest = null;
	    		}
        	}
	        
        }
    }

	/**
     * Shutdown this operator and close Elasticsearch API client.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Close connection to Elasticsearch server.
        client.close();
        
        super.shutdown();
    }
    
    /**
     * Get index from either the indexName or indexNameAttribute. indexNameAttribute 
     * overrides indexName.
     * @param tuple
     * @return
     */
    private String getIndex(Tuple tuple) {
    	if (indexNameAttribute != null) {
    		String index = indexNameAttribute.getValue(tuple);
    		if (!index.isEmpty()) {
    			return index;
    		}
    	} else if (indexName != null) {
    		return indexName;
    	}
    	return null;
    }
    
    /**
     * Get type from either the typeName or typeNameAttribute. typeNameAttribute 
     * overrides typeName.
     * @param tuple
     * @return
     */
    private String getType(Tuple tuple) {
    	if (typeNameAttribute != null) {
    		String index = typeNameAttribute.getValue(tuple);
    		if (!index.isEmpty()) {
    			return index;
    		}
    	} else if (typeName != null) {
    		return typeName;
    	}
    	return null;
    }
    
    /**
     * Get ID from either the idName or idNameAttribute. idNameAttribute 
     * overrides idName.
     * @param tuple
     * @return
     */
    private String getId(Tuple tuple) {
    	if (idNameAttribute != null) {
    		String index = idNameAttribute.getValue(tuple);
    		if (!index.isEmpty()) {
    			return index;
    		}
    	} else if (idName != null) {
    		return idName;
    	}
    	return null;
    }
    
    /**
     * Get timestamp from either the timestampName or timestampValueAttribute. timestampValueAttribute 
     * overrides timestampName.
     * @param tuple
     * @return
     */
    private Long getTimestampValue(Tuple tuple) {
    	if (timestampValueAttribute != null) {
    		return timestampValueAttribute.getValue(tuple);
    	}
    	return null;
    }
    
    /**
     * Get and set size metrics.
     * @param indexToInsert
     * @param typeToInsert
     * @param idToInsert
     */
    private void getAndSetSizeMetrics(String indexToInsert, String typeToInsert, String idToInsert) {
		SearchResponse sr = client.prepareSearch(indexToInsert)
											.setTypes(typeToInsert)
											.setQuery(QueryBuilders.matchAllQuery())
											.addAggregation(AggregationBuilders.extendedStats(SIZE_METRICS_PROPERTY).field(SIZE_PROPERTY))
											.get();
		
		ExtendedStats sizeMetrics = sr.getAggregations().get(SIZE_METRICS_PROPERTY);
		
		avgInsertSizeBytes.setValue((long)sizeMetrics.getAvg());
		maxInsertSizeBytes.setValue((long)sizeMetrics.getMax());
		minInsertSizeBytes.setValue((long)sizeMetrics.getMin());
		sumInsertSizeBytes.setValue((long)sizeMetrics.getSum());
    }
    
    /**
     * Check if operator is currently connected to Elasticsearch server. If not, try connecting.
     * @param client
     * @throws IOException 
     */
    private Boolean connectedToElasticsearch(String indexToInsert, String typeToInsert) throws IOException {
    	
    	// Keep trying to reconnect until reconnectionPolicyCount met.
    	long reconnectionAttempts = 0;
    	reconnectionCount.setValue(0);
    	while (reconnectionAttempts < reconnectionPolicyCount) {
	    	try {
				// Create index, if it doesn't exist.
	    		boolean indexExists = client.admin().indices().prepareExists(indexToInsert).execute().actionGet().isExists();
				if (!indexExists) {
					client.admin().indices().prepareCreate(indexToInsert).get();
				}
				
				// Set _size mapping, if enabled in parameters.
				if (sizeMetricsEnabled) {
					try {
						XContentBuilder builder = XContentFactory.jsonBuilder()
																.startObject()
																	.startObject("_size")
																		.field("enabled", true)
																	.endObject()
																.endObject();
						
						client.admin().indices().preparePutMapping(indexToInsert).setType(typeToInsert).setSource(builder).get();
						mapperSizeInstalled = true;
						
					} catch (MapperParsingException e) {
						_trace.error(e);
						mapperSizeInstalled = false;
					}
				}
				
				if (bulkRequest == null) {
					bulkRequest = client.prepareBulk();
				}
				
				isConnected.setValue(1);
				return true;
				
	        } catch (InvalidIndexNameException | NoNodeAvailableException e) {
	        	_trace.error(e);
	        	
	        	isConnected.setValue(0);
	        	reconnectionAttempts++;
	        	reconnectionCount.increment();
	        }
    	}
    	
    	_trace.error("Reconnection policy count, " + reconnectionPolicyCount + ", reached. Operator still not connected to server.");
    	return false;
    }
    
    /**
     * isConnected metric describes current connection status to Elasticsearch server.
     * @param isConnected
     */
    @CustomMetric(name = "isConnected", kind = Metric.Kind.GAUGE,
    		description = "Describes whether we are currently connected to Elasticsearch server.")
    public void setIsConnected(Metric isConnected) {
    	this.isConnected = isConnected;
    }
    
    /**
     * totalFailedRequests describes the number of failed inserts/gets over the lifetime of the operator.
     * @param totalFailedRequests
     */
    @CustomMetric(name = "totalFailedRequests", kind = Metric.Kind.COUNTER,
    		description = "The number of failed inserts/gets over the lifetime of the operator.")
    public void setTotalFailedRequests(Metric totalFailedRequests) {
    	this.totalFailedRequests = totalFailedRequests;
    }
    
    /**
     * numInserts metric describes the number of times a record has been successfully written.
     * @param numInserts
     */
    @CustomMetric(name = "numInserts", kind = Metric.Kind.COUNTER,
    		description = "The number of times a record has been written to the Elasticsearch server.")
    public void setNumInserts(Metric numInserts) {
    	this.numInserts = numInserts;
    }
    
    /**
     * isConnected metric describes current connection status to Elasticsearch server.
     * @param reconnectionCount
     */
    @CustomMetric(name = "reconnectionCount", kind = Metric.Kind.COUNTER,
    		description = "The number of times the operator has tried reconnecting to the server since the last successful connection.")
    public void setReconnectionCount(Metric reconnectionCount) {
    	this.reconnectionCount = reconnectionCount;
    }
    
    /**
     * The average size of inserted records, in bytes, within the current type.
     * @param avgInsertSizeBytes
     */
    @CustomMetric(name = "avgInsertSizeBytes", kind = Metric.Kind.GAUGE,
    		description = "The average size of inserted records, in bytes (aggregated over all documents within the current type).")
    public void setAvgInsertSizeBytes(Metric avgInsertSizeBytes) {
    	this.avgInsertSizeBytes = avgInsertSizeBytes;
    }
    
    /**
     * The maximum size of any inserted records, in bytes, within the current type.
     * @param maxInsertSizeBytes
     */
    @CustomMetric(name = "maxInsertSizeBytes", kind = Metric.Kind.GAUGE,
    		description = "The maximum size of any inserted records, in bytes (aggregated over all documents within the current type).")
    public void setMaxInsertSizeBytes(Metric maxInsertSizeBytes) {
    	this.maxInsertSizeBytes = maxInsertSizeBytes;
    }

    /**
     * The minimum size of any inserted records, in bytes, within the current type.
     * @param minInsertSizeBytes
     */
    @CustomMetric(name = "minInsertSizeBytes", kind = Metric.Kind.GAUGE,
    		description = "The minimum size of any inserted records, in bytes (aggregated over all documents within the current type).")
    public void setMinInsertSizeBytes(Metric minInsertSizeBytes) {
    	this.minInsertSizeBytes = minInsertSizeBytes;
    }
    
    /**
     * The total size of all inserted records, in bytes, within the current type.
     * @param sumInsertSizeBytes
     */
    @CustomMetric(name = "sumInsertSizeBytes", kind = Metric.Kind.GAUGE,
    		description = "The total size of all inserted records, in bytes (aggregated over all documents within the current type).")
    public void setSumInsertSizeBytes(Metric sumInsertSizeBytes) {
    	this.sumInsertSizeBytes = sumInsertSizeBytes;
    }
    
}
