package org.apache.zookeeper.server;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SiftThread {	

	public final ConcurrentLinkedQueue<Request> reqQueue = 
			new ConcurrentLinkedQueue<Request>();
	private int numThreads;
	private List<DracoClient> dracoClients = new ArrayList<DracoClient>();

	public SiftThread (int numThreads) {
		this.numThreads = numThreads;
	}

	public void launchSiftClients() {
		for (int i = 0; i < numThreads; i++) {
			DracoClient dc = new DracoClient(this, i);
			dc.start();
			dracoClients.add(dc);
		}		
	}
}
