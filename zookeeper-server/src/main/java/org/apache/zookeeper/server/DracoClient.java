package org.apache.zookeeper.server;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.txn.CreateTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DracoClient implements Runnable {
	private static final Logger LOG = 
			LoggerFactory.getLogger(DracoClient.class);

	private SiftThread st;
	
	private Socket socket = null;
	private Thread t;
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

    public DracoClient(SiftThread st, int threadNum) {
    	int port = 5500;
    	this.threadName = "DracoClient" + threadNum;
    	this.st = st;
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
    
	public void start() {
        if (t == null) {
            t = new Thread (this, threadName);
            t.start();
        }
    }

	public void run() {
		LOG.info("Running thread " + threadName);
		while (true) {
			Request rq = this.st.reqQueue.poll();
			try {
				if (rq != null) {
					if (debug) LOG.info("Draco req " + rq.type);
					if (rq.type == OpCode.create) {
						CreateRequest create2Request = new CreateRequest();
						ByteBufferInputStream.byteBuffer2Record(rq.rq, create2Request);
						rq.dracoPath = create2Request.getPath();
						this.put(rq.dracoPath, 
			            		new String(create2Request.getData()));						
						rq.lock.lock();
						try {
							rq.dracoDone = true;
							rq.dracoWait.signal();
						} finally {
							rq.lock.unlock();
						}
					} else if (rq.type == OpCode.getData) {
						GetDataRequest getDataRequest = new GetDataRequest();                
		                ByteBufferInputStream.byteBuffer2Record(rq.request,
		                        getDataRequest);
			            rq.dracoReturnVal = this.get(getDataRequest.getPath());
			            rq.lock.lock();
			            try {
			            	rq.dracoDone = true;
							rq.dracoWait.signal();
						} finally {
							rq.lock.unlock();
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
