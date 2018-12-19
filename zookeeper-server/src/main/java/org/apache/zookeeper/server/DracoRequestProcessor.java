package org.apache.zookeeper.server;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
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
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.quorum.CommitProcessor;
import org.apache.zookeeper.txn.CreateTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DracoRequestProcessor extends ZooKeeperThread 
	implements RequestProcessor {
	private static final Logger LOG = 
			LoggerFactory.getLogger(DracoRequestProcessor.class);
	
	//private Socket socket = null;
	private LinkedBlockingQueue<Socket> sockets = new LinkedBlockingQueue<Socket>();
	int socket_num = 12;
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
    protected WorkerService finalPool;

    public DracoRequestProcessor(RequestProcessor nextProcessor, String id) {
    	super("DracoRequestProcessor:" + id);
    	int port = 5500;
    	this.nextProcessor = nextProcessor;
    	try {
    		for (int i = 0; i < socket_num; i++) {
				Socket socket = new Socket(InetAddress.getLocalHost(), port);
				socket.setTcpNoDelay(true);
				sockets.add(socket);
    		}
		} catch (UnknownHostException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
			//e.printStackTrace();
		}
    }    
    
	public void run() {
		LOG.info("Running thread " + threadName);
		while (!stopped) {
			Request rq;
			Socket socket; 
			try {
				rq = this.queuedRequests.take();	
				socket = sockets.take();
				sendToNextProcessor(rq, socket);
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
        
        int numFinalThreads = numWorkerThreads;
        LOG.info("Num threads: " + numFinalThreads);

        if (workerPool == null) {
            workerPool = new WorkerService(
                "CommitProcWork", numWorkerThreads, true);
        }
        if (finalPool == null) {
        	finalPool = new WorkerService(
                    "FinalProcWork", numFinalThreads, true);
        }
        super.start();
    }
	
	/**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request, Socket socket) {
		workerPool.schedule(new DracoWorkRequest(request, socket), request.sessionId); 
    }
    
    private class DracoWorkRequest extends WorkerService.WorkRequest {
        private final Request rq;
        private final Socket socket;

        DracoWorkRequest(Request request, Socket socket) {
            this.rq = request;
            this.socket = socket;
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
        
        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor,"
                          + " unable to continue.");
                DracoRequestProcessor.this.halt();                
            }
        }

        public void doWork() throws RequestProcessorException {
        	if (debug) LOG.info("Draco req " + rq.type);
        	try {
				if (rq.type == OpCode.create) {
					CreateRequest create2Request = new CreateRequest();
					ByteBufferInputStream.byteBuffer2Record(rq.rq, create2Request);
					rq.dracoPath = create2Request.getPath();
					put(rq.dracoPath, new String(create2Request.getData()));						
					rq.dracoDone = true;
				} else if (rq.type == OpCode.setData) {
					SetDataRequest setDataRequest = new SetDataRequest();
					ByteBufferInputStream.byteBuffer2Record(rq.rq, setDataRequest);
					rq.dracoPath = setDataRequest.getPath();
					put(rq.dracoPath, new String(setDataRequest.getData()));						
					rq.dracoDone = true;
				} else if (rq.type == OpCode.getData) {
					GetDataRequest getDataRequest = new GetDataRequest();                
	                ByteBufferInputStream.byteBuffer2Record(rq.request,
	                        getDataRequest);
		            rq.dracoReturnVal = get(getDataRequest.getPath());
		            rq.dracoDone = true;
				}
        	} catch (IOException e) {
				e.printStackTrace();
			}
        	DracoRequestProcessor.this.sockets.add(socket);
        	DracoRequestProcessor.this.finalPool.schedule(new FinalWorkRequest(rq), rq.sessionId); 
               
        }
    }
    
    private class FinalWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        FinalWorkRequest(Request request) {
            this.request = request;
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
