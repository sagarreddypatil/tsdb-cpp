#pragma once

#include "fmapvec.hpp"
#include <unordered_map>

template<typename T>
class Table {
    struct Entry {
        uint64_t timestamp;
        T value;
    };

    std::shared_ptr<FileMappedVector<Entry>> data;


public:
    Table(std::string loc) {
        data = std::make_shared<FileMappedVector<Entry>>(loc);
    }

    void append(uint64_t timestamp, const T& value) {
        data->append({timestamp, value});
    }

    T operator[](size_t idx) {
        return data->operator[](idx).value;
    }

    size_t size() {
        return data->size();
    }

    size_t locate(uint64_t timestamp) {
        // tail optimized binary search
        // we know that the data is sorted by timestamp

        return _locate(timestamp, 0, data->size() - 1);
    }

    private:
    size_t _locate(uint64_t timestamp, size_t start, size_t end) {
        if (start == end) {
            return start;
        }

        size_t mid = (start + end) / 2;

        if (data->operator[](mid).timestamp >= timestamp && data->operator[](mid - 1).timestamp < timestamp) {
            return mid;
        }

        if (data->operator[](mid).timestamp < timestamp) {
            return _locate(timestamp, mid + 1, end);
        }

        return _locate(timestamp, start, mid);
    }
};

template<typename T>
class Database {
    std::string loc;

public:
    std::unordered_map<std::string, Table<T>> tables;

    Database(std::string loc) : loc(loc) {
        // loc names a folder, create if doesn't exist

        if(!std::filesystem::exists(loc)) {
            std::filesystem::create_directory(loc);
        }

        // each file is a table
        for (const auto & entry : std::filesystem::directory_iterator(loc)) {
            std::string table_name = entry.path().filename().string();
            tables[table_name] = Table<T>(entry.path().string());
        }
    }

    void make_table(std::string name) {
        if (tables.find(name) != tables.end()) {
            std::cerr << "Error: table " << name << " already exists" << std::endl;
            return;
        }

        tables[name] = Table<T>(loc + "/" + name);
    }

    Table<T> get_table(std::string name) {
        if (tables.find(name) == tables.end()) {
            std::cerr << "Error: table " << name << " does not exist" << std::endl;
            return nullptr;
        }

        return tables[name];
    }

};