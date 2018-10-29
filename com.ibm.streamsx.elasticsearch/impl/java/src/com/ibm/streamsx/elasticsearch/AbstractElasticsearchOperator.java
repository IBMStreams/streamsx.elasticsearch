//
// ****************************************************************************
// * Copyright (C) 2018, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.elasticsearch.client.ClientMetrics;
import com.ibm.streamsx.elasticsearch.client.Configuration;

/**
 * This class should be used as parent for all ES operators
 * it holds common operator parameters and helper/default methods
 */
@Libraries("opt/downloaded/*")
public class AbstractElasticsearchOperator extends AbstractOperator
{
	
	/**
	 * Metrics
	 */
	private Metric isConnected;
	private Metric totalFailedRequests;
	private Metric reconnectionCount;
	
	// parameter related members ----------------------------------------------------------------------
	
	// the host and port of the elastic search server (deprecated)
	private String hostName = null;
	private int hostPort = 0;
	
	// the new nodeList parameter
	private String nodeList = null;
	
	// reconnection policy 
	private int reconnectionPolicyCount = 1;
	
	// The username for basic authentication 
	private String userName = null;
	
	// The password for basic authentication
	private String password = null;
	
	// The name of the applicaton config object
	private String appConfigName = null;

	// is ssl enabled
	private boolean sslEnabled = false;
	
	// shall we enable SSL debug output ?
	private boolean sslDebug = false;

	// trust all certificate, use for debugging only
	private boolean sslTrustAllCertificates = false;
	
	// do not verify certificate hostname, use for debugging only
	private boolean sslVerifyHostname = true;
	
	// the name of the file containing the trusted certificates
	private String sslTrustStore = null;
	
	// the password for the truststore file
	private String sslTrustStorePassword = null;

	// timeouts for connection handling
	private int readTimeout = 5000;
	private int connectionTimeout = 20000;
	private long maxConnectionIdleTime = 1500L;
	
	// internal members ------------------------------------------------------------------------------
	
	// Logger for tracing.
    private static Logger logger = Logger.getLogger(AbstractElasticsearchOperator.class.getName());
	
    // data from application config object
    Map<String, String> appConfig = null;
    
    // the operator context
    private OperatorContext opContext = null;
    
	// operator methods ------------------------------------------------------------------------------

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
	        super.initialize(context);
	        opContext = context;
	        logger.trace("initialize AbstractElasticsearchOperator");
	        loadAppConfig(context);
	}
	
    // helper methods -----------------------------------------------------------------------------------
    
	/**
	 * construct a new configuration object from the data known so far.
	 * It uses the operator parameters where available. Or the entries from the application config, if available
	 * @return the Config used as input for the ES client implementation
	 */
	protected Configuration getClientConfiguration() {
		Configuration cfg = Configuration.getDefaultConfiguration();

		String hostname = (null != appConfig.get("hostName")) ? appConfig.get("hostName") : hostName;
		String hostport = (null != appConfig.get("hostPort")) ? appConfig.get("hostPort") : Integer.toString(hostPort);
		String nodelistAppConfig = appConfig.get("nodeList"); // value from application configuration
		String nodeslist = nodeList; // value from operator parameter
		if (null != nodelistAppConfig) {
			logger.info("nodeList (appConfig) contains: " + nodelistAppConfig.toString());
			nodeslist = nodelistAppConfig; // app config overwrites operator parameter
		}
		
		if (hostname != null || !hostport.equals("0")) {
			if (null != nodeslist) {
				logger.warn("Parameter nodeList is specified but will be ignored because one or both parameters 'hostName' and 'hostPort' are also specified");
			}
			if (null == hostname) hostname="localhost";
			if (hostport.equals("0")) hostport="9200";
			cfg.addNode(hostname,hostport);
		} else if (null != nodeslist) {
			String[] nodes = nodeslist.split(",");
			for (String n: nodes) {
				String[] vals = n.split(":");
				if (vals.length == 2) {
					cfg.addNode(vals[0],vals[1]);
				} else if (vals.length == 1) {
					cfg.addNode(vals[0],"9200");
				} else {
					logger.error("Parameter nodeList contains invalid entries : " + nodeslist.toString());
					throw new RuntimeException("Parameter nodeList contains invalid entries");
				}
			}
		} else {	// none of the 3 parameters nodeList,hostName,hostPort are set, so default to localhost:9200
			cfg.addNode("localhost","9200");
		}
		
		if (null != appConfig.get("userName")) {
			cfg.setUserName(appConfig.get("userName"));
		} else {
			cfg.setUserName(userName);
		}

		if (null != appConfig.get("password")) {
			cfg.setPassword(appConfig.get("password"));
		} else {
			cfg.setPassword(password);
		}

		if (null != appConfig.get("reconnectionPolicyCount")) {
			cfg.setReconnectionPolicyCount(Integer.parseInt(appConfig.get("reconnectionPolicyCount")));
		} else {
			cfg.setReconnectionPolicyCount(reconnectionPolicyCount);
		}

		if (null != appConfig.get("sslEnabled")) {
			cfg.setSslEnabled(Boolean.parseBoolean(appConfig.get("sslEnabled")));
		} else {
			cfg.setSslEnabled(sslEnabled);
		}

		if (null != appConfig.get("sslDebug")) {
			cfg.setSslDebug(Boolean.parseBoolean(appConfig.get("sslDebug")));
		} else {
			cfg.setSslDebug(sslDebug);
		}

		if (null != appConfig.get("sslTrustAllCertificates")) {
			cfg.setSslTrustAllCertificates(Boolean.parseBoolean(appConfig.get("sslTrustAllCertificates")));
		} else {
			cfg.setSslTrustAllCertificates(sslTrustAllCertificates);
		}

		if (null != appConfig.get("sslVerifyHostname")) {
			cfg.setSslVerifyHostname(Boolean.parseBoolean(appConfig.get("sslVerifyHostname")));
		} else {
			cfg.setSslVerifyHostname(sslVerifyHostname);
		}

		String trustFile = null;
		if (null != appConfig.get("sslTrustStore")) {
			trustFile = appConfig.get("sslTrustStore");
		} else {
			trustFile = sslTrustStore;
		}
		if (null != trustFile) {
			// make path absolute
			File t = new File(trustFile);
			if (!t.isAbsolute()) {
				File appDir = opContext.getPE().getApplicationDirectory();
				trustFile = appDir.getPath() + "/etc/" + trustFile;
			}
		}
		cfg.setSslTrustStore(trustFile);
		
		if (null != appConfig.get("sslTrustStorePassword")) {
			cfg.setSslTrustStorePassword(appConfig.get("sslTrustStorePassword"));
		} else {
			cfg.setSslTrustStorePassword(sslTrustStorePassword);
		}
		
		if (null != appConfig.get("readTimeout")) {
			cfg.setReadTimeout(Integer.parseInt(appConfig.get("readTimeout")));
		} else {
			cfg.setReadTimeout(readTimeout);
		}

		if (null != appConfig.get("connectionTimeout")) {
			cfg.setConnectionTimeout(Integer.parseInt(appConfig.get("connectionTimeout")));
		} else {
			cfg.setConnectionTimeout(connectionTimeout);
		}

		if (null != appConfig.get("maxConnectionIdleTime")) {
			cfg.setMaxConnectionIdleTime(Long.parseLong(appConfig.get("maxConnectionIdleTime")));
		} else {
			cfg.setMaxConnectionIdleTime(maxConnectionIdleTime);
		}
		
		return cfg;
	}
	
	/**
	 * read the application config into a map
	 * @param context the operator context 
	 */
    protected void loadAppConfig(OperatorContext context) {
    	
    	// if no appconfig name is specified, create empty map
        if (appConfigName == null) {
        	appConfig = new HashMap<String,String>();
        	return;
        }

        appConfig = context.getPE().getApplicationConfiguration(appConfigName);
        if (appConfig.isEmpty()) {
            logger.warn("APPLICATION_CONFIG_NOT_FOUND_OR_EMPTY: " + appConfigName);
        }
        
        for (Map.Entry<String, String> kv : appConfig.entrySet()) {
        	logger.trace("Found application config entry : " + kv.getKey() + "=" + kv.getValue());
        }
        
    }
    
    // Connection related parameters -------------------------------------------------------------- 
    
    @Parameter(name="hostName", optional=true,
    	description="Specifies the hostname of the Elasticsearch server. The default is 'localhost'. "
    	+ "If you specify a protocol prefix like 'http://' or 'https://' it is ignored, because the protocol is determined by "
    	+ "the parameter 'sslEnabled'. If that is set to true, HTTPS is used, otherwise HTTP is used. "
    	+ "This parameter can be overwritten by the application configuration. "
    	+ "NOTE: this parameter is deprecated, use the 'nodeList' parameter instead. "
    )
    public void setHostName(String hostName) {
		if (hostName.matches("^http://.*")) {
			this.hostName = hostName.substring(7);
		} else if (hostName.matches("^https://.*")) {
			this.hostName = hostName.substring(8);
		} else {
	 		this.hostName = hostName;
 		}
    }

    @Parameter(name="hostPort", optional=true,
    	description="Specifies the REST port of the Elasticsearch server. The default port is 9200. "
    	+ "This parameter can be overwritten by the application configuration. "
    	+ "NOTE: this parameter is deprecated, use the 'nodeList' parameter instead. "
    )
	public void setHostPort(int hostPort) {
		this.hostPort = hostPort;
	}

    @Parameter(name="nodeList", optional=true,
       	description="Specifies a list of Elasticsearch nodes to use for operations. The nodes must be part of the same cluster. "
       	+ "The format is a comma separated list of hostname:port entries. For example: 'host1:9200,host2:9200'. This parameter is ignored if one of the parameters "
       	+ "hostName or hostPort are also specified. If none of these parameters are specified, the default nodelist is 'localhost:9200'. This parameter can be "
       	+ "overwritten by the application configuration."
    )
    public void setNodeList(String nodeList) {
  		this.nodeList = nodeList;
    }
    
    @Parameter(name="reconnectionPolicyCount", optional=true,
		description="Specifies the number of reconnection attemps to th Elasticsearch server, upon disconnection. "
	    + "If more than one node is specified in the 'nodeList' parameter, all remaining nodes are tried immediately, before the reconnection count starts. "
		+ "This parameter can be overwritten by the application configuration. "
	)
	public void setReconnectionPolicyCount(int reconnectionPolicyCount) {
		this.reconnectionPolicyCount = reconnectionPolicyCount;
	}
   
    @Parameter(name="userName", optional=true,
    	description="The username used for HTTP basic authentication. If parameter 'sslEnabled' is false, the username is transmitted in cleartext. "
    	+ "This parameter can be overwritten by the application configuration. "
    )
    public void setUsername(String userName) {
    	this.userName = userName;
    }

    @Parameter(name="password", optional=true,
    	description="The password used for HTTP basic authentication. If parameter 'sslEnabled' is false, the password is transmitted in cleartext. "
    	+ "This parameter can be overwritten by the application configuration. "
    )
    public void setPassword(String password) {
    	this.password = password;
    }
    
    @Parameter(name="sslEnabled", optional=true,
    	description="Indicate if SSL/TLS shall be used to connect to the nodes. The default is 'false'. "
    	+ "This parameter can be overwritten by the application configuration."	
    )
    public void setSslEnabled(boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}
    
    @Parameter(name="sslDebug", optional=true,
       	description="If SSL/TLS protocol debugging is enabled, all protocol data and information is logged to the console. "
       	+ "Use this to debug TLS connection problems. The default is 'false'. "
    	+ "This parameter can be overwritten by the application configuration."	
    )
	public void setSslDebug(boolean sslDebug) {
		this.sslDebug = sslDebug;
	}

    @Parameter(name="sslTrustAllCertificates", optional=true,
       	description="If set to true, the SSL/TLS layer will not verify the server certificate chain. "
       	+ "WARNING: this is unsecure and should only be used for debugging purposes. The default is 'false'. "
    	+ "This parameter can be overwritten by the application configuration."	
    )
	public void setSslTrustAllCertificates(boolean sslTrustAllCertificates) {
		this.sslTrustAllCertificates = sslTrustAllCertificates;
	}

    @Parameter(name="sslVerifyHostname", optional=true,
       	description="If set to false, the SSL/TLS layer will not verify the hostname in the server certificate against the actual name of the server host. "
       	+ "WARNING: this is unsecure and should only be used for debugging purposes. The default is 'true'. "
    	+ "This parameter can be overwritten by the application configuration."	
    )
	public void setSslVerifyHostname(boolean sslVerifyHostname) {
		this.sslVerifyHostname = sslVerifyHostname;
	}

    @Parameter(name="sslTrustStore", optional=true,
    	description="Specifies the name of a file containing trusted certificates. The format is the common Java truststore format, "
    	+ "and you can use the JAVA keytool command to create and manage truststore files. "
        + "Use this parameter if the Elasticsearch server certificate is signed by a CA that is not trusted per default with your current Java version, "
        + "or uses a self-signed certificate. "
        + "This parameter can be overwritten by the application configuration."	
    )
    public void setSslTrustStore(String sslTrustStore) {
		this.sslTrustStore = sslTrustStore;
	}

    @Parameter(name="sslTrustStorePassword", optional=true,
    	description="Specify the password used to access the Truststore file, specified in the 'sslTrustStore' parameter. "
        + "This parameter can be overwritten by the application configuration. "	
    )
	public void setSslTrustStorePassword(String sslTrustStorePassword) {
		this.sslTrustStorePassword = sslTrustStorePassword;
	}

    @Parameter(name="readTimeout", optional=true,
       	description="The timeout for waiting for a REST response from the server node. Specified in milliseconds. "
       	+ "The default value is 5000 (5 seconds). "
        + "This parameter can be overwritten by the application configuration. "	
    )
    public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}
    
    @Parameter(name="connectionTimeout", optional=true,
    	description="The timeout for waiting on establishment of the TCP connection to the server node. Specified in milliseconds. "
    	+ "The default value is 20000 (20 seconds). "
        + "This parameter can be overwritten by the application configuration. "	
    )
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

    @Parameter(name="maxConnectionIdleTime", optional=true,
      	description="If the TCP connection to a server node is not used for that time, it is closed. Specified in milliseconds. "
      	+ "The default value is 1500 (1.5 seconds). "
        + "This parameter can be overwritten by the application configuration. "	
    )
	public void setMaxConnectionIdleTime(long maxConnectionIdleTime) {
		this.maxConnectionIdleTime = maxConnectionIdleTime;
	}

	@Parameter(
		name="appConfigName", optional = true,
		description="Specifies the name of the application configuration that contains Elasticsearch connection related configuration parameters. The keys in the application configuration have the same name as the operator parameters. "
		+ " The following keys are supported: userName, password, hostName, hostPort, nodeList, reconnectionPolicyCount, sslEnabled, sslDebug, sslTrustAllCertificates, sslVerifyHostname, sslTrustStore, sslTrustStorePassword. "
		+ " If a value is specified in the application configuration and as operator parameter, the application configuration parameter value takes precedence. "
	)
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}	
   
    // parameter related getters ---------------------------------------------------------------------

	public String getHostName() {
		return hostName;
	}

	public int getHostPort() {
		return hostPort;
	}

	public int getReconnectionPolicyCount() {
		return reconnectionPolicyCount;
	}

	public Map<String, String> getAppConfig() {
		return appConfig;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}

	public String getAppConfigName() {
		return appConfigName;
	}
	
	public boolean isSslEnabled() {
		return sslEnabled;
	}

	public boolean isSslDebug() {
		return sslDebug;
	}

	public boolean isSslTrustAllCertificates() {
		return sslTrustAllCertificates;
	}

	public boolean isSslVerifyHostname() {
		return sslVerifyHostname;
	}

	public String getSslTrustStore() {
		return sslTrustStore;
	}

	public String getSslTrustStorePassword() {
		return sslTrustStorePassword;
	}
	
	public int getReadTimeout() {
		return readTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public long getMaxConnectionIdleTime() {
		return maxConnectionIdleTime;
	}

	// metrics ----------------------------------------------------------------------------------------------------------------

	protected void updateMetrics (ClientMetrics clientMetrics) {
		// handle common metrics here 
		if (clientMetrics.getIsConnected()) {
			this.isConnected.setValue(1);
		}
		else {
			this.isConnected.setValue(0);
		}
		this.totalFailedRequests.setValue(clientMetrics.getTotalFailedRequests());
		this.reconnectionCount.setValue(clientMetrics.getReconnectionCount());
	}  	
	
    /**
     * isConnected metric describes current connection status to Elasticsearch server.
     * @param isConnected
     */
    @CustomMetric(name = "isConnected", kind = Metric.Kind.GAUGE,
    	description = "Describes whether we are currently connected to Elasticsearch server. "
    	+ "This is set to 0 after all cluster nodes became unreachable and the maximum reconnection attempts were unsuccessful. Otherwise the value is 1. "
    )
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
     * isConnected metric describes current connection status to Elasticsearch server.
     * @param reconnectionCount
     */
    @CustomMetric(name = "reconnectionCount", kind = Metric.Kind.COUNTER,
    	description = "The number of times the operator has tried reconnecting to the cluster since the last successful connection. "
    	+ "If there are multiple nodes in the cluster, reconnecting to a different node in the cluster is not counted here. "
    )
    public void setReconnectionCount(Metric reconnectionCount) {
    	this.reconnectionCount = reconnectionCount;
    }	
	
}
