#include <iostream>
#include <map>
#include <string>
#include <stdint.h>

class KVServer {
	std::map<std::string, std::string> kvm;

public:
	
	KVServer(int serverID) {
		std::cout << "KVServer constructor\n";
	}

	void put(std::string key, std::string value) {
		std::cout << "PUT\n";
		this->kvm[key] = value;
	}

	std::string get(std::string key) {
		std::cout << "GET\n";
		return this->kvm[key];
	}

};