//
// ****************************************************************************
// * Copyright (C) 2018, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Data structure to hold client configuration data
 */
public class Configuration
{
	// kept from original implementation
	private int reconnectionPolicyCount = 1;
	
	// the nodes
	private List<String> nodeList = new ArrayList<String>();
	
	// credentials (valid for all nodes)
	private String userName = null;
	private String password = null;
	
	// parameters for node discovery 
	private boolean nodeDiscoveryEnabled = false;
	private Long nodeDiscoveryInterval = 0l;
	private String nodeDiscoveryFilter = null;

	// ssl related parameters 
	private boolean sslEnabled = false;
	private String sslTrustStore = null;
	private String sslTrustStorePassword = null;
	private String sslKeyStore = null;
	private String sslKeyStorePassword = null;
	private boolean sslTrustAllCertificates = false;
	private boolean sslVerifyHostname = true;
	private boolean sslDebug = false;
	
	private int readTimeout = 5000;
	private int connectionTimeout = 20000;
	private long maxConnectionIdleTime = 1500L;
	
	public static Configuration getDefaultConfiguration() {
		return new Configuration();
	}

	public void addNode(String host, String port) {
		nodeList.add(host + ":" + port);
	}
	
	public List<String> getNodeList() {
		List<String> result = new ArrayList<String>();
		if (nodeList.size() == 0) {
			throw new RuntimeException("Invalid Client configuration: empty nodeList");
		}
		String prefix = sslEnabled ? "https://" : "http://";
		
		for (String s : nodeList) {
			result.add(prefix + s);
		}
		return result;
	}

	public int getReconnectionPolicyCount() {
		return reconnectionPolicyCount;
	}

	public void setReconnectionPolicyCount(int reconnectionPolicyCount) {
		this.reconnectionPolicyCount = reconnectionPolicyCount;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isNodeDiscoveryEnabled() {
		return nodeDiscoveryEnabled;
	}

	public void setNodeDiscoveryEnabled(boolean nodeDiscoveryEnabled) {
		this.nodeDiscoveryEnabled = nodeDiscoveryEnabled;
	}

	public Long getNodeDiscoveryInterval() {
		return nodeDiscoveryInterval;
	}

	public void setNodeDiscoveryInterval(Long nodeDiscoveryInterval) {
		this.nodeDiscoveryInterval = nodeDiscoveryInterval;
	}

	public String getNodeDiscoveryFilter() {
		return nodeDiscoveryFilter;
	}

	public void setNodeDiscoveryFilter(String nodeDiscoveryFilter) {
		this.nodeDiscoveryFilter = nodeDiscoveryFilter;
	}

	public boolean isSslEnabled() {
		return sslEnabled;
	}

	public void setSslEnabled(boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	public String getSslTrustStore() {
		return sslTrustStore;
	}

	public void setSslTrustStore(String sslTrustStore) {
		this.sslTrustStore = sslTrustStore;
	}

	public String getSslTrustStorePassword() {
		return sslTrustStorePassword;
	}

	public void setSslTrustStorePassword(String sslTrustStorePassword) {
		this.sslTrustStorePassword = sslTrustStorePassword;
	}

	public String getSslKeyStore() {
		return sslKeyStore;
	}

	public void setSslKeyStore(String sslKeyStore) {
		this.sslKeyStore = sslKeyStore;
	}

	public String getSslKeyStorePassword() {
		return sslKeyStorePassword;
	}

	public void setSslKeyStorePassword(String sslKeyStorePassword) {
		this.sslKeyStorePassword = sslKeyStorePassword;
	}

	public boolean isSslVerifyHostname() {
		return sslVerifyHostname;
	}

	public void setSslVerifyHostname(boolean sslVerifyHostname) {
		this.sslVerifyHostname = sslVerifyHostname;
	}

	public boolean isSslDebug() {
		return sslDebug;
	}

	public void setSslDebug(boolean sslDebug) {
		this.sslDebug = sslDebug;
	}

	public boolean isSslTrustAllCertificates() {
		return sslTrustAllCertificates;
	}

	public void setSslTrustAllCertificates(boolean sslTrustAllCertificates) {
		this.sslTrustAllCertificates = sslTrustAllCertificates;
	}

	public long getMaxConnectionIdleTime() {
		return maxConnectionIdleTime;
	}	

	public int getReadTimeout() {
		return readTimeout;
	}
	
	public int getConnectionTimeout() {
		return connectionTimeout;
	}	
	
	public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public void setMaxConnectionIdleTime(long maxConnectionIdleTime) {
		this.maxConnectionIdleTime = maxConnectionIdleTime;
	}
	
	@Override
	public String toString() {
		return "Configuration [reconnectionPolicyCount=" + reconnectionPolicyCount + ", nodeList=" + nodeList
				+ ", userName=" + userName + ", password=" + password + ", nodeDiscoveryEnabled=" + nodeDiscoveryEnabled
				+ ", nodeDiscoveryInterval=" + nodeDiscoveryInterval + ", nodeDiscoveryFilter=" + nodeDiscoveryFilter
				+ ", sslEnabled=" + sslEnabled + ", sslTrustStore=" + sslTrustStore + ", sslTrustStorePassword="
				+ sslTrustStorePassword + ", sslKeyStore=" + sslKeyStore + ", sslKeyStorePassword="
				+ sslKeyStorePassword + ", sslTrustAllCertificates=" + sslTrustAllCertificates + ", sslVerifyHostname="
				+ sslVerifyHostname + ", sslDebug=" + sslDebug + ", readTimeout=" + readTimeout + ", connectionTimeout="
				+ connectionTimeout + ", maxConnectionIdleTime=" + maxConnectionIdleTime + ", getNodeList()="
				+ getNodeList() + "]";
	}

}
