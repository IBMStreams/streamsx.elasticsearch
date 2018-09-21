package com.ibm.streamsx.elasticsearch.client;

import java.io.IOException;

import org.apache.log4j.Logger;
import com.ibm.streamsx.elasticsearch.client.Configuration;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * implementation for the JEST client library
 *
 */
public class JESTClient implements Client {
	
	// external properties
	private Logger logger = null;
	private Configuration cfg = null;
	
	// internal properties
	private JestClient client = null;

	public JESTClient(Configuration config) {
		super();
		this.cfg = config;
	}

	@Override
	public void setConfiguration(Configuration config) {
		cfg = config;
		logger.trace("Configuration set: " + config.toString() );
	}

	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	@Override
	public boolean validateConfiguration() {
		// TODO implement config validation
		return true;
	}

	@Override
	public boolean init() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                               .Builder(cfg.getNodeList())
                               .multiThreaded(true)
                               .build());
        client = factory.getObject();
        return true;
	}

	@Override
	public void close() {
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Object getRawClient() {
		return client;
	}

}
