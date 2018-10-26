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
 * Data structure to hold client status metrics
 */
public class ClientMetrics
{
	private boolean isConnected = true;
	private Long numInserts = 0l;
	private Long totalFailedRequests = 0l;
	private Long reconnectionCount = 0l;

	public static ClientMetrics getClientMetrics() {
		return new ClientMetrics();
	}

	public boolean getIsConnected() {
		return isConnected;
	}

	public void setIsConnected(boolean isConnected) {
		this.isConnected = isConnected;
	}

	public long getNumInserts() {
		return numInserts;
	}

	public void setNumInserts(long numInserts) {
		this.numInserts = numInserts;
	}

	public void incrementNumInserts() {
		this.numInserts++;
	}	
	
	public long getTotalFailedRequests() {
		return totalFailedRequests;
	}

	public void setTotalFailedRequests(long totalFailedRequests) {
		this.totalFailedRequests = totalFailedRequests;
	}

	public void incrementTotalFailedRequests() {
		this.totalFailedRequests++;
	}	
	
	public long getReconnectionCount() {
		return reconnectionCount;
	}

	public void setReconnectionCount(long reconnectionCount) {
		this.reconnectionCount = reconnectionCount;
	}	

	public void incrementReconnectionCount() {
		this.reconnectionCount++;
	}	
}
