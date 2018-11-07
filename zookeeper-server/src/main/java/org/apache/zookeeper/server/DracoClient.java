package org.apache.zookeeper.server;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.zookeeper.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DracoClient {
	private static final Logger LOG = 
			LoggerFactory.getLogger(DracoClient.class);

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

    public DracoClient() {
    	int port = 5500;
    	try {
			socket = new Socket(InetAddress.getLocalHost(), port);
                socket.setTcpNoDelay(true);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
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
    
}
