package org.apache.zookeeper.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.*;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
//import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DracoRequestProcessor extends ZooKeeperCriticalThread implements
RequestProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(DracoRequestProcessor.class);
	
	private final ZooKeeperServer zks;
	
	RequestProcessor nextProcessor;
	
	private Map<String, byte[]> dracoKv = new HashMap<String, byte[]>();
	
	public DracoRequestProcessor(ZooKeeperServer zks,
	    RequestProcessor nextProcessor) {
		super("DracoRequestProcessor:" + zks.getServerId(), zks
		        .getZooKeeperServerListener());
		this.zks = zks;
		this.nextProcessor = nextProcessor;
	}
	
	@Override
	public void processRequest(Request request) 
			throws RequestProcessorException {
		LOG.info("Draco process");
		try {
			switch (request.type) {
				case OpCode.create: {
		            this.putDraco(request, request.getHdr(), request.getTxn());
		            break;
		        }
				case OpCode.getData: {
	                this.getDraco(request);
	                break;
	            } default: {
	            	LOG.info("Draco does not support this action: " + request.type);
	            }
			}
		} catch (Exception e) {
			LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
		}
		nextProcessor.processRequest(request);
	}
	
	private void putDraco(Request request, TxnHeader hdr, Record txn) {
		CreateTxn createTxn = (CreateTxn) txn;
		dracoKv.put(createTxn.getPath(), createTxn.getData());
        LOG.info("PUT Draco Path: " + createTxn.getPath());
        LOG.info("PUT Draco Data: " + createTxn.getData().toString());
	}
	
	private void getDraco(Request request) throws IOException {
		GetDataRequest getDataRequest = new GetDataRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request,
                getDataRequest);
		String key = getDataRequest.getPath();
		LOG.info("GET Draco Path: " + key);
		LOG.info("GET Draco Data: " + dracoKv.get(key).toString());
	}
	
	@Override
	public void shutdown() {
		LOG.info("Draco shutting down");
		nextProcessor.shutdown();
	}

}
