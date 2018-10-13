package org.apache.zookeeper.server;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class KVServer {
	public interface CLibrary extends Library {
		CLibrary INSTANCE = (CLibrary) Native.loadLibrary("draco", CLibrary.class);
		
		Pointer KVServer_ctor(int serverID);
		
		void KVServer_put(Pointer self, String key, String value);
		
		String KVServer_get(Pointer self, String key);
	}
	
	private Pointer self;
	
	public KVServer(int serverID) {
		self = CLibrary.INSTANCE.KVServer_ctor(serverID);
	}
	
	public void put(String key, String value) {
		CLibrary.INSTANCE.KVServer_put(self, key, value);
	}
	
	public String get(String key) {
		return CLibrary.INSTANCE.KVServer_get(self, key);
	}
}
