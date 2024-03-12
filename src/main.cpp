#include <iostream>
#include <filesystem>
#include <vector>
#include <map>
#include <unordered_map>

#include "fmapvec.hpp"
#include <stdint.h>

template<typename T>
class Database {
    std::string loc;

public:
    struct Entry {
        uint64_t timestamp;
        T data;
    };

    std::unordered_map<std::string, std::shared_ptr<FileMappedVector<Entry>>> tables;

    Database(std::string loc) : loc(loc) {
        // loc names a folder, create if doesn't exist

        if(!std::filesystem::exists(loc)) {
            std::filesystem::create_directory(loc);
        }

        // each file is a table
        for (const auto & entry : std::filesystem::directory_iterator(loc)) {
            std::string table_name = entry.path().filename().string();
            tables[table_name] = std::make_shared<FileMappedVector<Entry>>(entry.path().string());
        }
    }

    void make_table(std::string name) {
        if (tables.find(name) != tables.end()) {
            std::cerr << "Error: table " << name << " already exists" << std::endl;
            return;
        }

        tables[name] = std::make_shared<FileMappedVector<Entry>>(loc + "/" + name);
    }

    std::shared_ptr<FileMappedVector<Entry>> get_table(std::string name) {
        if (tables.find(name) == tables.end()) {
            std::cerr << "Error: table " << name << " does not exist" << std::endl;
            return nullptr;
        }

        return tables[name];
    }

};

int main(int argc, char* argv[]) {
    // std::vector<std::string> args(argv + 1, argc + argv);
    // std::map<std::string, std::string> options;

    // for (uint i = 0; i < args.size(); i++) {
    //     if (args[i].substr(0, 2) == "--") {
    //         if (i + 1 >= args.size()) {
    //             std::cerr << "Error: option " << args[i] << " requires an argument" << std::endl;
    //             return 1;
    //         }

    //         options[args[i].substr(2)] = args[i + 1];
    //         i++;
    //     }
    // }

    struct datapoint {
        int x;
        int y;
    };

    Database<datapoint> db("db");
    db.make_table("test");
    db.get_table("test")->append({1, 2});
}