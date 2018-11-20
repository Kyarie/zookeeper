package org.apache.zookeeper.server;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class SiftThread {	

	public final LinkedBlockingQueue<Request> reqQueue = 
			new LinkedBlockingQueue<Request>();
	private int numThreads;
	private List<DracoRequestProcessor> dracoClients = new ArrayList<DracoRequestProcessor>();
	
	public SiftThread () {
		this.numThreads = 5;
	}

	public void launchSiftClients() {
		/*
		for (int i = 0; i < numThreads; i++) {
			DracoRequestProcessor dc = new DracoRequestProcessor(this, i);
			dc.start();
			dracoClients.add(dc);
		}*/
	}
}
