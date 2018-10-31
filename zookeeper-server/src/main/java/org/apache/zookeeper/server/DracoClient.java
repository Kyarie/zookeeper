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

    public DracoClient() {
    	int port = 5000;
    	try {
			socket = new Socket(InetAddress.getLocalHost(), port);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
			//e.printStackTrace();
		}
    }
    
    public String get(String key) {
    	int req_type = READ_REQUEST;
    	sendInt(req_type);
    	
    	int key_len = key.length();
    	sendInt(req_type);
    	
    	sendStr(key);
    	
    	int value_len = recvInt();
    	
    	String value = recvStr();
    	
    	return value;
    }
    
    public void put(String key, String value) {
    	int req_type = WRITE_REQUEST;
    	sendInt(req_type);
    	
    	int key_len = key.length();
    	sendInt(key_len);
    	
    	sendStr(key);
    	
    	int value_len = value.length();
    	sendInt(value_len);
    	
    	sendStr(value);
    	
    	int ack = recvInt();
    	
    	if (LOG.isDebugEnabled()) {
			LOG.debug("draco put ack: " + ack);
		}
    }
    
    private void sendInt(int data) {
    	int modData = data;
    	try {
    		if (LOG.isDebugEnabled()) {
    			LOG.debug("draco send: " + data);
    		}
			dout = new DataOutputStream(socket.getOutputStream());
			dout.write(modData);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}    			
    }
    
    private int recvInt() {
    	int result = 0;
    	try {
			din = new DataInputStream(socket.getInputStream());
			result = din.readInt();
			if (LOG.isDebugEnabled()) {
				LOG.debug("draco get: " + result);
    		}	    	
			LOG.info("draco get: " + result);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}    			
    	return result;
    }
    
    private void sendStr(String data) {   	
    	try {
    		if (LOG.isDebugEnabled()) {
    			LOG.debug("draco send: " + data);
    		}
			out = new PrintStream(socket.getOutputStream());
			out.println(data);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}    			
    }
    
    private String recvStr() {
    	String value = "";  
    	try {
			in = new InputStreamReader(socket.getInputStream());
			BufferedReader br = new BufferedReader(in);
			value = br.readLine();
			if (LOG.isDebugEnabled()) {
				LOG.info("draco get: " + value);
    		}	    	
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
    	return value;
    }

    private int htonl(int value) {
    	return ByteBuffer.allocate(4).putInt(value)
    			.order(ByteOrder.nativeOrder()).getInt(0);
    }
    
}
