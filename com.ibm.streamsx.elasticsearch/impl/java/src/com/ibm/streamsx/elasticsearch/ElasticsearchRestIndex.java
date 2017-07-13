//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.log4j.Logger;
import com.ibm.json.java.JSONObject;
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
import com.ibm.streamsx.elasticsearch.internal.SizeMapping;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.ExtendedStatsAggregation;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;

@PrimitiveOperator(
		name="ElasticsearchRestIndex",
		namespace="com.ibm.streamsx.elasticsearch",
		description=ElasticsearchRestIndex.DESC_OPERATOR
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
public class ElasticsearchRestIndex extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------

	static final String DESC_OPERATOR = 
			"The ElasticsearchRestIndex operator receives incoming tuples and outputs "
			+ "the attribute's name-value pairs to an Elasticsearch database.\\n"
			+ "\\n"
			+ "The ElasticsearchRestIndex requires a hostname and hostport of an "
			+ "Elasticsearch server to connect to.\\n"
			+ "\\n"
			+ "By default, the hostname is 'localhost', and the hostport "
			+ "is 9200. This configuration can be changed in the parameters.\\n"
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
		if (hostName.matches("^http://.*")) {
			this.hostName = hostName;
		} else {
	 		this.hostName = "http://" + hostName;
 		}
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
	private static Logger _trace = Logger.getLogger(ElasticsearchRestIndex.class.getName());
	
	/**
	 * Property names for size metrics in Elasticsearch.
	 */
	private static String SIZE_METRICS_PROPERTY = "_size_metrics";

	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------

	/**
	 * Elasticsearch parameters.
	 */
	private String hostName = "http://localhost";
	private int hostPort = 9200;
	
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
	 * Elasticsearch Jest API.
	 */
	private JestClient client;
	private Builder bulkBuilder;
	private int currentBulkSize = 0;
	
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
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                               .Builder(hostName + ":" + hostPort)
                               .multiThreaded(true)
                               .build());
        
        client = factory.getObject();
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
    	
    	// Add attribute names/values to jsonDocuments.
    	JSONObject jsonDocuments = new JSONObject();
    	for(String attributeName : attributeNames) {
    		
    		// Skip attributes used explicitly for defining index, type, id, and timestamps.
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
    			jsonDocuments.put(attributeName, tuple.getObject(attributeName).toString());
    		} else {
    			jsonDocuments.put(attributeName, tuple.getObject(attributeName));
    		}
    	}
    	
    	// Add timestamp, if enabled.
    	if (storeTimestamps) {
    		DateFormat df = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss.SSSZZ");
    		
    		String timestampToInsert;
    		if (timestampValueAttribute != null) {
    			long timestamp = getTimestampValue(tuple).longValue();
    			timestampToInsert = df.format(new Date(timestamp));
    		} else {
    			timestampToInsert = df.format(new Date(System.currentTimeMillis()));
    		}
    		
    		jsonDocuments.put(timestampName, timestampToInsert);
    	}
        	
    	// Get index, type, and ID.
    	String indexToInsert = getIndex(tuple);
    	String typeToInsert = getType(tuple);
    	String idToInsert = getId(tuple);
    	
    	if (indexToInsert == null || typeToInsert == null) {
    		throw new Exception("Index and type must be defined.");
    	}
    	
    	if (connectedToElasticsearch(indexToInsert, typeToInsert)) {
        	
    		// Add jsonDocuments to bulkBuilder.
        	String source = jsonDocuments.toString();
        	
        	if (idToInsert != null) {
        		bulkBuilder.addAction(new Index.Builder(source).index(indexToInsert).type(typeToInsert).id(idToInsert).build());
        	} else {
        		bulkBuilder.addAction(new Index.Builder(source).index(indexToInsert).type(typeToInsert).build());
        	}
        	
        	currentBulkSize++;
    	
        	// If bulk size met, output jsonFields to Elasticsearch.
        	if(currentBulkSize >= bulkSize) {
	        	Bulk bulk = bulkBuilder.build();
	        	BulkResult result;
	        	
	        	try {
		        	result = client.execute(bulk);
	        	} catch (NoHttpResponseException e) {
	        		_trace.error(e);
	        		return;
	        	}
	        	
    			if (result.isSucceeded()) {
    				long currentNumInserts = numInserts.getValue();
    				numInserts.setValue(currentNumInserts + bulkSize);
    				currentBulkSize = 0;
    			} else {
    				for (BulkResultItem item : result.getItems()) {
    					if (item.error != null) {
    						numInserts.increment();
    					} else {
    						totalFailedRequests.increment();
    					}
    				}
    			}
    			
    			// Clear bulkBuilder. Gets recreated in connectedToElasticsearch().
    			bulkBuilder = null;
    			
    			// Get size metrics for current type.
    			if (sizeMetricsEnabled && mapperSizeInstalled) {
	    			getAndSetSizeMetrics(indexToInsert, typeToInsert, idToInsert);
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
        client.shutdownClient();
        
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
     * @throws IOException 
     */
    private void getAndSetSizeMetrics(String indexToInsert, String typeToInsert, String idToInsert) throws IOException {
    	String query = "{\n" +
                "    \"query\" : {\n" +
                "        \"match_all\" : {}\n" +
                "    },\n" +
                "    \"aggs\" : {\n" +
                "        \"size_metrics\" : {\n" +
                "            \"extended_stats\" : {\n" +
                "                \"field\" : \"_size\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
		
        Search search = new Search.Builder(query)
                .addIndex(indexToInsert)
                .addType(typeToInsert)
                .build();
        
        SearchResult searchResult = client.execute(search);
        
        if (searchResult.isSucceeded()) {
	        ExtendedStatsAggregation sizeMetrics = searchResult.getAggregations().getExtendedStatsAggregation(SIZE_METRICS_PROPERTY);
			
	        if (sizeMetrics != null) {
	        	if (sizeMetrics.getAvg() != null) {
	    			avgInsertSizeBytes.setValue(sizeMetrics.getAvg().longValue());
	        	}
	        	if (sizeMetrics.getMax() != null) {
	        		maxInsertSizeBytes.setValue(sizeMetrics.getMax().longValue());
	        	}
	        	if (sizeMetrics.getMin() != null) {
	        		minInsertSizeBytes.setValue(sizeMetrics.getMin().longValue());
	        	}
	        	if (sizeMetrics.getSum() != null) {
	        		sumInsertSizeBytes.setValue(sizeMetrics.getSum().longValue());
	        	}
	        }
        }
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
				// Create index if it doesn't exist.
	    		boolean indexExists = client.execute(new IndicesExists.Builder(indexToInsert).build()).isSucceeded();
				if (!indexExists) {
					JestResult result = client.execute(new CreateIndex.Builder(indexToInsert).build());
					
					if (!result.isSucceeded()) {
						isConnected.setValue(0);
						return false;
					}
				}

				// Enable _size mapping.
				if (sizeMetricsEnabled) {
					SizeMapping sizeMapping = new SizeMapping.Builder(indexToInsert, typeToInsert, true).build();
					JestResult result = client.execute(sizeMapping);
					
					if (result.isSucceeded()) {
						mapperSizeInstalled = true;
					} else {
						_trace.error("Mapper size plugin was not detected. Please try restarting the Elasticsearch server after install.");
					}
				}

				// Reset bulkBuilder.
				if (bulkBuilder == null) {
					bulkBuilder = new Bulk.Builder()
								  .defaultIndex(indexToInsert)
								  .defaultType(typeToInsert);
				}
				
				isConnected.setValue(1);
				return true;
				
	        } catch (HttpHostConnectException e) {
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
