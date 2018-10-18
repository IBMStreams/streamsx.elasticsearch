package com.ibm.streamsx.elasticsearch.client;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.HttpHeaders;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.log4j.Logger;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.core.Index;

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
	private Builder bulkBuilder = null;
	private int bulkSize = 0;
	
	// http basic authentication 
	private boolean useBasicAuth = false;
	private String authHeader = null;

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
	public boolean init() throws Exception {

	    JestClientFactory factory = new JestClientFactory();
	    client = null;
	       
	    // create basic authentication header if needed
        if (cfg.getUserName() != null) {
        	String credentials = cfg.getUserName() + ":" + cfg.getPassword();
        	byte[] encodedCredentials = Base64.getEncoder().encode(credentials.getBytes(StandardCharsets.ISO_8859_1));
        	authHeader = "Basic " + new String(encodedCredentials);
        	useBasicAuth = true;
        }
	    
		// use ssl 
	    // TODO add error checking and logging here 
		if (cfg.isSslEnabled()) {

			// in case we are running on an IBM Java, where TLSv1.2 is not enabled per default, set this property
			// the IBM Cloud Elasticsearch compose service needs TLS1.2, otherwise it will close connection immediately
			System.setProperty("com.ibm.jsse2.overrideDefaultTLS","true");
			
			// enable debug, if needed
			if (cfg.isSslDebug()) {
				System.setProperty("javax.net.debug","true");
			}
			
			SSLContext sslContext = null;
			SSLConnectionSocketFactory sslSocketFactory = null;
			
			// trust all certificates , use use supplied truststore, or use java defaults
			if (cfg.isSslTrustAllCertificates()) {
				try {
					sslContext = new SSLContextBuilder().loadTrustMaterial(new TrustStrategy() {
						@Override
						public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
							// trust all certificates
							return true;
						}
					}).build();
				} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw e;
				}
			} else if (cfg.getSslTrustStore() != null) {
				File trustFile = new File(cfg.getSslTrustStore());
				if (cfg.getSslTrustStorePassword() != null) {
					try {
						sslContext = new SSLContextBuilder().loadTrustMaterial(trustFile,cfg.getSslTrustStorePassword().toCharArray()).build();
					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException
							| CertificateException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						throw e;
					}
				} else {
					try {
						sslContext = new SSLContextBuilder().loadTrustMaterial(trustFile).build();
					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException
							| CertificateException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						throw e;
					}
				}
			} else {
				try {
					sslContext = SSLContext.getDefault();
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw e;
				}
			}
			
			// verify hostname or not
			if (!cfg.isSslVerifyHostname()) {
				HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;
				sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
			} else {
				sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
			}

       	  	factory.setHttpClientConfig(new HttpClientConfig.Builder(cfg.getNodeList())
        	  		.sslSocketFactory(sslSocketFactory)
        	  		.multiThreaded(false)
        	  		.build());
		
	    // use HTTP only 
		} else {
       	  	factory.setHttpClientConfig(new HttpClientConfig.Builder(cfg.getNodeList())
       	  			.multiThreaded(false)
        	  		.build());
		}
        
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
	public void bulkIndexAddDocument(String document, String indexToInsert, String typeToInsert, String idToInsert) {
		// reset bulk builder
		if (null == bulkBuilder) {
			bulkBuilder = new Bulk.Builder().defaultIndex(indexToInsert).defaultType(typeToInsert);
		}

		if (null != typeToInsert) {
			if (idToInsert != null) {
				bulkBuilder.addAction(new Index.Builder(document).index(indexToInsert).type(typeToInsert).id(idToInsert).build());
			} else {
				bulkBuilder.addAction(new Index.Builder(document).index(indexToInsert).type(typeToInsert).build());
			}
		} else {
			if (idToInsert != null) {
				bulkBuilder.addAction(new Index.Builder(document).index(indexToInsert).id(idToInsert).build());
			} else {
				bulkBuilder.addAction(new Index.Builder(document).index(indexToInsert).build());
			}
		}
		bulkSize++;
	}

	@Override
	public void bulkIndexSend() {

		if (null == bulkBuilder) {
			logger.info("the bulk is empty, nothing to send");
			return;
		}
		
		if (useBasicAuth) {
			bulkBuilder.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
		}
		
		Bulk bulk = bulkBuilder.build();
		BulkResult result = null;
		boolean sendError = false;
		try {
			result = client.execute(bulk);
		} catch (NoHttpResponseException e) {
			logger.error("HTTP error. Cannot send bulk to server. Exception : " + e.getMessage());
			sendError = true;
		} catch (IOException e) {
			// TODO : read timeout can happen here 
			logger.error("IO error. Cannot send bulk to server. Exception : " + e.getMessage());
			e.printStackTrace();
			sendError = true;
		}

		int failedInserts = 0;
		if (!sendError && result.isSucceeded()) {
			logger.info("bulk send successfully, size = " + Integer.toString(bulkSize));
		} else {
			if (result != null) {
				for (BulkResultItem item : result.getItems()) {
					if (item.error != null) {
						failedInserts++;
						logger.error("bulk item indexing failed. " + item.error);
					}
				}
				logger.info("bulk send partially successful. Total items = " + Integer.toString(bulkSize) + ", failed = " + Integer.toString(failedInserts));
			} else {
				logger.error("bulk send failed. bulk size = " + Integer.toString(bulkSize));
			}
		}

		bulkBuilder = null;
		bulkSize = 0;
		
	}

}
