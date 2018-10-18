//
// ****************************************************************************
// * Copyright (C) 2018, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONObject;
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
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.elasticsearch.client.Client;
import com.ibm.streamsx.elasticsearch.client.Configuration;
import com.ibm.streamsx.elasticsearch.client.JESTClient;

@PrimitiveOperator(name="ElasticsearchIndex", namespace="com.ibm.streamsx.elasticsearch", description=ElasticsearchIndex.operatorDescription)
@InputPorts({@InputPortSet(
		id="0",
		description=ElasticsearchIndex.iport0Description,
		cardinality=1,
		optional=false,
		windowingMode=WindowMode.NonWindowed,
		windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)
})
public class ElasticsearchIndex extends AbstractElasticsearchOperator
{
	// operator parameter members --------------------------------------------------------------------------- 
	
	private int bulkSize = 1;
	
	private String indexName;
	private TupleAttribute<Tuple, String> indexNameAttribute;

	private String typeName;
	private TupleAttribute<Tuple, String> typeNameAttribute;

	private String idName;
	private TupleAttribute<Tuple, String> idNameAttribute;
	
	private boolean storeTimestamps = false;	
	private String timestampName = "timestamp";
	private TupleAttribute<Tuple, Long> timestampValueAttribute;
	
	// internal members -------------------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger logger = Logger.getLogger(ElasticsearchIndex.class.getName());
	
	/**
	 * Elasticsearch Client API.
	 */
	private Client client;
	private int currentBulkSize = 0;
	private Configuration config = null;
	
	/**
	 * Metrics
	 */
	private Metric isConnected;
	private Metric totalFailedRequests;
	private Metric numInserts;
	private Metric reconnectionCount;
	
	/**
     * Initialize this operator and create Elasticsearch client to send get requests to.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

        // Construct a new client config object
        config = getClientConfiguration();
        logger.debug(config.toString());
        // TODO remove after testing
        System.out.println(config.toString());
        
        // create client 
        // TODO add robust error checking here
        client = new JESTClient(config);
        client.setLogger(logger);
        client.init();
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

    	// Add document to bulk
    	String source = jsonDocuments.toString();
    	client.bulkIndexAddDocument(source,indexToInsert,typeToInsert,idToInsert);
    	currentBulkSize++;
    	
    	// send bulk if needed
    	if (currentBulkSize == bulkSize) {
    		client.bulkIndexSend();
    		currentBulkSize = 0;

    		// TODO : handle metrics here 
    		// Metric isConnected;
    		// Metric totalFailedRequests;
    		// Metric numInserts;
    		// Metric reconnectionCount;

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
        // shutdown client
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
 
    // metrics ----------------------------------------------------------------------------------------------------------------
    
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
    
    // operator parameters setters ------------------------------------------------------------------------------------------------------
    
	@Parameter(name="indexName", optional=true,
		description="Specifies the name for the index."
	)
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	
	@Parameter(name="indexNameAttribute", optional=true,
		description="Specifies the attribute providing the index names."
	)
	public void setIndexNameAttribute(TupleAttribute<Tuple, String> indexNameAttribute) {
		this.indexNameAttribute = indexNameAttribute;
	}
	
	@Parameter(name="typeName", optional=true,
		description="Specifies the name for the type."
	)
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
	@Parameter(name="typeNameAttribute", optional=true,
		description="Specifies the attribute providing the type names."
	)
	public void setTypeNameAttribute(TupleAttribute<Tuple, String> typeNameAttribute) {
		this.typeNameAttribute = typeNameAttribute;
	}
	
	@Parameter(name="idName", optional=true,
		description="Specifies the name for the id. If not specified, id is auto-generated."
	)
	public void setIdName(String idName) {
		this.idName = idName;
	}
	
	@Parameter(name="idNameAttribute", optional=true,
		description="Specifies the attribute providing the ID names."
	)
	public void setIdNameAttribute(TupleAttribute<Tuple, String> idNameAttribute) {
		this.idNameAttribute = idNameAttribute;
	}
	
	@Parameter(name="storeTimestamps", optional=true,
		description="Enables storing timestamps."
	)
	public void setStoreTimestamps(boolean storeTimestamps) {
		this.storeTimestamps = storeTimestamps;
	}
	
	@Parameter(name="timestampName", optional=true,
		description="Specifies the name for the timestamp attribute."
	)
	public void setTimestampName(String timestampName) {
		this.timestampName = timestampName;
	}
	
	@Parameter(name="timestampValueAttribute", optional=true,
		description="Specifies the attribute providing the timestamp values."
	)
	public void setTimestampValueAttribute(TupleAttribute<Tuple, Long> timestampValueAttribute) throws IOException {
		this.timestampValueAttribute = timestampValueAttribute;
	}
	
	@Parameter(name="bulkSize", optional=true,
		description="Specifies the size of the bulk to submit to Elasticsearch."
	)
	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}
	
	// operator and port documentation -------------------------------------------------------------------------------------------------------

	static final String operatorDescription = 
			"The ElasticsearchIndex operator receives incoming tuples and outputs "
			+ "the attribute's name-value pairs to an Elasticsearch database.\\n"
			+ "\\n"
			+ "The ElasticsearchIndex requires a hostname and hostport of an "
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
	
	static final String iport0Description = "Port that ingests tuples"
			;
 
}
