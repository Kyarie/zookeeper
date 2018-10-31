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
    	get("hi");
    }
    
    public String get(String key) {
    	int req_type = htonl(READ_REQUEST);
    	String value = "";  
    	send(key);
    	return value;
    }
    
    public void put(String key, String value) {
    	int req_type = htonl(WRITE_REQUEST);
    	send(key);
    }
    
    public String send(String data) {    	
    	String value = "";
    	PrintStream out = null;
    	InputStreamReader in = null;
    	try {
    		LOG.info("draco send: " + data);
			out = new PrintStream(socket.getOutputStream());
			out.println(data);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}    	
		try {
			in = new InputStreamReader(socket.getInputStream());
			BufferedReader br = new BufferedReader(in);
			value = br.readLine();
	    	LOG.info("draco get: " + value);
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
