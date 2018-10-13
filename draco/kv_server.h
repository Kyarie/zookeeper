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

	void put(char *key, char *value) {
		std::cout << "PUT " << key << " " << value << "\n";
		std::string sk(key);
		std::string sv(value);
		this->kvm[sk] = sv;
		std::cout << this->kvm[sk] << "\n";
	}

	char *get(char* key) {
		std::cout << "GET " << key << "\n";
		std::string sk(key);
		std::cout << this->kvm[sk] << "\n";
		char *c = new char[sk.length()+1];
		return c;
	}

};