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
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.elasticsearch.client.Configuration;

/**
 * This class should be used as parent for all ES operators
 * it holds common operator parameters and helper/default methods
 */
@Libraries("opt/downloaded/*")
public class AbstractElasticsearchOperator extends AbstractOperator
{
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
			logger.error("nodeList (appConfig) contains: " + nodelistAppConfig.toString());
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
    	description="Specifies the hostname of the Elasticsearch server (default: localhost)."
    )
    public void setHostName(String hostName) {
		if (hostName.matches("^http://.*")) {
			this.hostName = hostName.substring(7);
		} else {
	 		this.hostName = hostName;
 		}
    }

    @Parameter(name="hostPort", optional=true,
    	description="Specifies the hostport of the Elasticsearch server (default: 9200)."
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
		description="Specifies the number of times to attempt reconnection to the "
		+ "Elasticsearch server, upon disconnection."
	)
	public void setReconnectionPolicyCount(int reconnectionPolicyCount) {
		this.reconnectionPolicyCount = reconnectionPolicyCount;
	}
   
    @Parameter(name="userName", description="The username used to authenticate against the database.", optional=true)
    public void setUsername(String userName) {
    	this.userName = userName;
    }

    @Parameter(name="password", description="The password for the username.", optional=true)
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
       	+ "This will help with debugging TLS connection problems. The default is 'false'. "
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
       	description="If set to false, the SSL/TLS layer will not verify the hostname in the certificate against the actual name of the server host. "
       	+ "WARNING: this is unsecure and should only be used for debugging purposes. The default is 'true'. "
    	+ "This parameter can be overwritten by the application configuration."	
    )
	public void setSslVerifyHostname(boolean sslVerifyHostname) {
		this.sslVerifyHostname = sslVerifyHostname;
	}

    @Parameter(name="sslTrustStore", optional=true,
    	description="This is the name of a file containing trusted certificates. The format is the common Java truststore format. "
        + "Use this parametere, if the server certificate is signed by a CA that is not trusted per default with your current Java version. "
        + "For example use it with self-signed certificates. This parameter can be overwritten by the application configuration."	
    )
    public void setSslTrustStore(String sslTrustStore) {
		this.sslTrustStore = sslTrustStore;
	}

    @Parameter(name="sslTrustStorePassword", optional=true,
    	description="If set to false, the SSL/TLS layer will not verify the hostname in the certificate against the actual name of the server host. "
        + "WARNING: this is unsecure and should only be used for debugging purposes. The default is 'true'. "
        + "This parameter can be overwritten by the application configuration."	
    )
	public void setSslTrustStorePassword(String sslTrustStorePassword) {
		this.sslTrustStorePassword = sslTrustStorePassword;
	}

    // other common parameters -----------------------------------------------------------------------
	
	@Parameter(
		name="appConfigName", optional = true,
		description="Specifies the name of the application configuration that contains Elasticsearch connection related configuration parameters. The keys in the application configuration have the same name as the operator parameters."
		+ " The following keys are supported: userName, password, hostName, hostPort, nodeList, reconnectionPolicyCount, sslEnabled, sslDebug, sslTrustAllCertificates, sslVerifyHostname, sslTrustStore, sslTrustStorePassword."
		+ " If a value is specified in the application configuration and as operator parameter, the application configuration parameter value takes precedence."
	)
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}	
	
	// checkers --------------------------------------------------------------------------------------
	
	@ContextCheck(compile = false, runtime = true)
    public static void runtimeChecker(OperatorContextChecker checker) {
		// StreamsHelper.validateOutputAttributeRuntime(checker, ERRCODE_ATTR_PARAM, null, MetaType.RSTRING);
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

}
