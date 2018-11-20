package org.apache.zookeeper.server;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.quorum.CommitProcessor;
import org.apache.zookeeper.txn.CreateTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DracoRequestProcessor extends ZooKeeperThread 
	implements RequestProcessor {
	private static final Logger LOG = 
			LoggerFactory.getLogger(DracoRequestProcessor.class);
	
	private Socket socket = null;
	int READ_REQUEST = 1;
	int WRITE_REQUEST = 2;
	int WRITE_OK = 1000;
	PrintStream out = null;
	InputStreamReader in = null;
	DataOutputStream dout = null;
	DataInputStream din = null;
	boolean debug = false;
    boolean lat_test = false;
    String threadName;
    
    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS =
        "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT =
        "zookeeper.commitProcessor.shutdownTimeout";

    /**
     * Incoming requests.
     */
    protected LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();

    RequestProcessor nextProcessor;

    protected volatile boolean stopped = false;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;

    public DracoRequestProcessor(RequestProcessor nextProcessor, String id) {
    	super("DracoRequestProcessor:" + id);
    	int port = 5500;
    	this.nextProcessor = nextProcessor;
    	try {
			socket = new Socket(InetAddress.getLocalHost(), port);
			socket.setTcpNoDelay(true);
		} catch (UnknownHostException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
			//e.printStackTrace();
		}
    }    
    
    public String get(String key) throws IOException {
    	int bufSize = 8 + key.length();
    	ByteBuffer buffer = ByteBuffer.allocate(bufSize);
    	buffer.putInt(READ_REQUEST);
    	buffer.putInt(key.length());
    	buffer.put(key.getBytes());
    	byte[] data = buffer.array();
    	if (debug) LOG.info("Sending data key " + key);
		socket.getOutputStream().write(data); 
		
    	byte[] valLenBt = new byte[4];
		int rc = socket.getInputStream().read(valLenBt);
		ByteBuffer valLenBuf = ByteBuffer.wrap(valLenBt);
		int valLen = valLenBuf.getInt();
		if (debug) LOG.info("value length " + valLen);
		
		byte[] valueBt = new byte[valLen];
		rc = socket.getInputStream().read(valueBt);
		String value = new String (valueBt);
		if (debug) LOG.info("value " + value);    	
    	return value;
    }

    public void put(String key, String value) throws IOException {
        long start = System.nanoTime(); 
    	int bufSize = 12 + key.length() + value.length();
    	ByteBuffer buffer = ByteBuffer.allocate(bufSize);
    	buffer.putInt(WRITE_REQUEST);
    	buffer.putInt(key.length());
    	buffer.put(key.getBytes());
    	buffer.putInt(value.length());
    	buffer.put(value.getBytes());
    	byte[] data = buffer.array();
    	if (debug) LOG.info("Sending data key " + key);
		socket.getOutputStream().write(data);    	
		
    	byte[] ackBt = new byte[4];
		int rc = socket.getInputStream().read(ackBt);
		ByteBuffer ackBuf = ByteBuffer.wrap(ackBt);
		int ack = ackBuf.getInt();
		if (debug) LOG.info("ack " + ack);
        if (lat_test) {
        	long end = System.nanoTime(); 
        	long microseconds = (end-start)/1000; 
        	LOG.info("Final Duration: " + microseconds); 
        }
       }
    
	public void run() {
		LOG.info("Running thread " + threadName);
		while (!stopped) {
			Request rq;
			try {
				rq = this.queuedRequests.take();
				if (debug) LOG.info("Draco req " + rq.type);
				if (rq.type == OpCode.create) {
					CreateRequest create2Request = new CreateRequest();
					ByteBufferInputStream.byteBuffer2Record(rq.rq, create2Request);
					rq.dracoPath = create2Request.getPath();
					this.put(rq.dracoPath, 
		            		new String(create2Request.getData()));						
					rq.lock.lock();
					rq.dracoDone = true;
				} else if (rq.type == OpCode.getData) {
					GetDataRequest getDataRequest = new GetDataRequest();                
	                ByteBufferInputStream.byteBuffer2Record(rq.request,
	                        getDataRequest);
		            rq.dracoReturnVal = this.get(getDataRequest.getPath());
		            rq.dracoDone = true;
				}
				sendToNextProcessor(rq);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		LOG.info("DracoRequestProcessor exited loop!");
	}
	
	@Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(
            ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(
            ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        if (workerPool == null) {
            workerPool = new WorkerService(
                "CommitProcWork", numWorkerThreads, true);
        }
        super.start();
    }
	
	/**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        workerPool.schedule(new DracoWorkRequest(request), request.sessionId);
    }
    
    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     */
    private class DracoWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        DracoWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor,"
                          + " unable to continue.");
                DracoRequestProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            nextProcessor.processRequest(request);
        }
    }
    
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        queuedRequests.add(request);
    }

    private void halt() {
        stopped = true;
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }
}
