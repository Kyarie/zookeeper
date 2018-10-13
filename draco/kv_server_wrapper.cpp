#include "kv_server.h"

#define DLL_EXPORT

extern "C" DLL_EXPORT KVServer *KVServer_ctor(int serverID);

KVServer *KVServer_ctor(int serverID) {
	return new KVServer(serverID);
}

extern "C" DLL_EXPORT void KVServer_put(KVServer *self, 
	std::string key, std::string value) {
	self->put(key, value);
}

extern "C" DLL_EXPORT std::string KVServer_get(KVServer *self, 
	std::string key) {
	return self->get(key);
}
