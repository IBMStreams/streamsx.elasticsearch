package com.ibm.streamsx.elasticsearch.client;

import org.apache.log4j.Logger;

/**
 * This is the interface between the Streams operators and the Elasticsearch 
 * client implementation. All code in the client namespace is supposed
 * to not have any dependencies or knowledge about Streams
 */
public interface Client
{
	/**
	 * The client shall log valuable information for debugging to this logger
	 */
	void setLogger(Logger logger);

	/**
	 * Store the configuration in the client 
	 * @param config client configuration
	 */
	void setConfiguration(Configuration config);
	
	/**
	 * check if the current configuration information is sufficient to perform operations against the database
	 * @return true if the configuration is ok, false otherwise
	 */
	boolean validateConfiguration();
	
	/**
	 * Initialize client. The method shall respect parameters from the configuration passed in.
	 * @throws Exception 
	 */
	boolean init() throws Exception;
	
	/**
	 * Add new document to an existing bulk, if the bulk does not exist it must be created
	 * @param idToInsert 
	 * @param typeToInsert 
	 * @param indexToInsert 
	 */
	void bulkIndexAddDocument(String document, String indexToInsert, String typeToInsert, String idToInsert);
	
	/**
	 * send the bulk to the ES server
	 */
	void bulkIndexSend();
	
	/**
	 * Close client, clean up any resources left over
	 */
	void close();
	
	/**
	 * Client metrics and statistics
	 */
	ClientMetrics getClientMetrics();
	
	/**
	 * reset bulk builder
	 */	
	void reset();
	
}
