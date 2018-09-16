//
// ****************************************************************************
// * Copyright (C) 2018, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch;

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
 
	// internal members ------------------------------------------------------------------------------
	
	// Logger for tracing.
    private static Logger logger = Logger.getLogger(AbstractElasticsearchOperator.class.getName());
	
    // data from application config object
    Map<String, String> appConfig = null;
    
	// operator methods ------------------------------------------------------------------------------

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
	        super.initialize(context);
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
		String nodelist = (null != appConfig.get("nodeList")) ? appConfig.get("nodeList") : nodeList;

		if (hostname != null || !hostport.equals("0")) {
			if (null != nodelist) {
				logger.warn("Parameter nodeList is specified but will be ignored because one or both parameters 'hostName' and 'hostPort' are also specified");
			}
			if (null == hostname) hostname="localhost";
			if (hostport.equals("0")) hostport="9200";
			cfg.addNode(hostname,hostport);
		} else if (null != nodeList) {
			String[] nodes = nodeList.split(",");
			for (String n: nodes) {
				String[] vals = n.split(":");
				if (vals.length == 2) {
					cfg.addNode(vals[0],vals[1]);
				} else if (vals.length == 1) {
					cfg.addNode(vals[0],"9200");
				} else {
					logger.error("Parameter nodeList contains invalid entries : " + nodeList.toString());
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

    // other common parameters -----------------------------------------------------------------------
    
	@Parameter(
		name="appConfigName", optional = true,
		description="Specifies the name of the application configuration that contains Cloudant connection related configuration parameters. The keys in the application configuration have the same name as the operator parameters."
		+ "The following keys are supported: url, username, password, databaseName."
		+ "If a value is specified in the application configuration and as operator parameter, the operator parameter value takes precedence."
	)
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}	
	
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

}
