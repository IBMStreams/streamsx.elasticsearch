package com.ibm.streamsx.elasticsearch.client;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
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
	    client = null;
	       
		// use ssl 
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
			
			// trust all certificates , or use defaults
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
				}
			} else {
				try {
					sslContext = SSLContext.getDefault();
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			// verify hostname or not
			if (!cfg.isSslVerifyHostname()) {
				HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;
				sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
			} else {
				sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
			}

	        if (cfg.getUserName() != null) {
	        	factory.setHttpClientConfig(new HttpClientConfig.Builder(cfg.getNodeList())
	        		.sslSocketFactory(sslSocketFactory)
//	        		.defaultCredentials(cfg.getUserName(),cfg.getPassword())
	        		.multiThreaded(false)
	        		.build());
	        } else {
        	  	factory.setHttpClientConfig(new HttpClientConfig.Builder(cfg.getNodeList())
        	  		.sslSocketFactory(sslSocketFactory)
        	  		.multiThreaded(false)
        	  		.build());
	        }
		
	    // use HTTP only 
		} else {
	        if (cfg.getUserName() != null) {
	        	factory.setHttpClientConfig(new HttpClientConfig.Builder(cfg.getNodeList())
//	        		.defaultCredentials(cfg.getUserName(),cfg.getPassword())
	        		.multiThreaded(false)
	        		.build());
	        } else {
        	  	factory.setHttpClientConfig(new HttpClientConfig.Builder(cfg.getNodeList())
        	  		.multiThreaded(false)
        	  		.build());
	        }
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
	public Object getRawClient() {
		return client;
	}

}
