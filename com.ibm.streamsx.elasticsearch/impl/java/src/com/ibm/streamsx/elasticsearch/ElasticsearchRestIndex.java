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

import org.apache.http.conn.HttpHostConnectException;
import org.apache.log4j.Logger;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
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

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.ExtendedStatsAggregation;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;

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
			description="Specifies the hostport of the Elasticsearch server (default: 9200)."
			)
	public void setHostPort(int hostPort) {
		this.hostPort = hostPort;
	}
	
	@Parameter(
			optional=false,
			description="Specifies the name for the index."
			)
	public void setIndexName(String index) {
		this.index = index;
	}
	
	@Parameter(
			optional=false,
			description="Specifies the name for the type."
			)
	public void setTypeName(String type) {
		this.type = type;
	}
	
	@Parameter(
			optional=true,
			description="Specified the name for the id. If not specified, id is auto-generated."
			)
	public void setIdName(String id) {
		this.id = id;
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

	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------

	/**
	 * Elasticsearch parameters.
	 */
	private String hostName = "http://localhost";
	private int hostPort = 9200;
	
	private String index;
	private String type;
	private String id;
	private String timestampName;
	
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
	 * Mapper size plugin is installed.
	 */
	private boolean mapperSizeInstalled = false;
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(ElasticsearchRestIndex.class.getName());
	
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
				       			//Per default this implementation will create no more than 2 concurrent connections per given route
				       			.defaultMaxTotalConnectionPerRoute(2)
				       			// and no more 20 connections in total
				       			.maxTotalConnection(10)
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
    	
    	// Collect fields to add to JSON output.
		StreamSchema schema = tuple.getStreamSchema();
    	Set<String> attributeNames = schema.getAttributeNames();
    	JSONObject jsonFields = new JSONObject();
    	
    	for(String attributeName : attributeNames) {
    		jsonFields.put(attributeName, tuple.getObject(attributeName));
    	}
        
    	// Add timestamp, if specified, for time-based queries.
    	if (timestampName != null) {
    		DateFormat df = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss.SSSZZ");
    		
    		if (attributeNames.contains(timestampName) && schema.getAttribute(timestampName).getType().getMetaType() == Type.MetaType.RSTRING
        			&& !tuple.getString(timestampName).equals("")) {
    			String timestampToInsert = tuple.getString(timestampName);
    			jsonFields.put(timestampName, df.format(timestampToInsert));
    		} else {
    			jsonFields.put(timestampName, df.format(new Date(System.currentTimeMillis())));
    		}
    	}
    	
    	String source = jsonFields.toString();
        
        // Output metrics to Elasticsearch.
        if (jsonFields != null) {
        	
        	// Get index name/type/id specified inside tuple (default: from params).
        	String indexToInsert = index;
        	String typeToInsert = type;
        	String idToInsert = null;
        	
        	if (attributeNames.contains(index) && schema.getAttribute(index).getType().getMetaType() == Type.MetaType.RSTRING
        			&& !tuple.getString(index).equals("")) {
        		indexToInsert = tuple.getString(index);
        		_trace.error("indexToInsert: " + indexToInsert);
        	}
        	
        	if (attributeNames.contains(type) && schema.getAttribute(type).getType().getMetaType() == Type.MetaType.RSTRING
        			&& !tuple.getString(type).equals("")) {
        		typeToInsert = tuple.getString(type);
        	}

        	if (id != null && attributeNames.contains(id) && schema.getAttribute(id).getType().getMetaType() == Type.MetaType.RSTRING
        			&& !tuple.getString(id).equals("")) {
        		idToInsert = tuple.getString(id);
        	}
        	
        	if (connectedToElasticsearch(indexToInsert, typeToInsert)) {
	        	
	    		// Add jsonFields to bulkBuilder.
	        	if (idToInsert != null) {
	        		bulkBuilder.addAction(new Index.Builder(source).index(indexToInsert).type(typeToInsert).id(idToInsert).build());
	        	} else {
	        		bulkBuilder.addAction(new Index.Builder(source).index(indexToInsert).type(typeToInsert).build());
	        	}
	        	
	        	currentBulkSize++;
        	
	        	// If bulk size met, output jsonFields to Elasticsearch.
	        	if(currentBulkSize == bulkSize) {
		        	Bulk bulk = bulkBuilder.build();
		        	JestResult result = client.execute(bulk);
	    			if (result.isSucceeded()) {
	    				long currentNumInserts = numInserts.getValue();
	    				numInserts.setValue(currentNumInserts + bulkSize);
	    				currentBulkSize = 0;
	    			} else {
	    				_trace.error(result.getErrorMessage());
//	    				for (BulkItemResponse item : result) {
//	    					if (!item.isFailed()) {
//	    						numInserts.increment();
//	    					} else {
//	    						totalFailedRequests.increment();
//	    					}
//	    				}
	    			}
	    			
	    			// Get/set size metrics for current type.
	    			if (mapperSizeInstalled) {
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
		    	        ExtendedStatsAggregation sizeMetrics = searchResult.getAggregations().getExtendedStatsAggregation("size_metrics");
		    			
		    			avgInsertSizeBytes.setValue((long)sizeMetrics.getAvg().doubleValue());
		    			maxInsertSizeBytes.setValue((long)sizeMetrics.getMax().doubleValue());
		    			minInsertSizeBytes.setValue((long)sizeMetrics.getMin().doubleValue());
		    			sumInsertSizeBytes.setValue((long)sizeMetrics.getSum().doubleValue());
	    			}
	    			
	    			// Clear bulkBuilder. Gets recreated in connectedToElasticsearch().
	    			bulkBuilder = null;
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
	    		GetMapping getMapping = new GetMapping.Builder().build();
	    		JestResult getResult = client.execute(getMapping);
				if (getResult.getJsonObject().getAsJsonObject(indexToInsert) == null) {
					
					JSONObject settings = new JSONObject();
					settings.put(typeToInsert
							, new JSONObject().put("_size"
									, new JSONObject().put("enabled", true)));
					PutMapping putMapping = new PutMapping.Builder(
							indexToInsert,
					        typeToInsert,
					        settings.toString()
					).build();
					JestResult result = client.execute(putMapping);
					_trace.error(settings.toString());
					
					mapperSizeInstalled = true;
					_trace.error("Index response is succeeded: " + result.isSucceeded());
					
					if (!result.isSucceeded()) {
						mapperSizeInstalled = false;

						result = client.execute(new CreateIndex.Builder(indexToInsert).build());
						_trace.error("Index response is succeeded: " + result.isSucceeded());
						
						if (!result.isSucceeded()) {
							return false;
						}
					}
				}

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
