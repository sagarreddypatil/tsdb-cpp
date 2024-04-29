#pragma once

extern "C" {
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <stdint.h>
#include <assert.h>
#include "fmapvec.h"
}

#include <functional>
#include <unordered_map>
#include <memory>
#include <filesystem>

namespace tsdb {

template<typename T>
class FileMappedVector {
    fmapvec_t vec;

public:
    FileMappedVector(std::string loc) {
        int fd = open(loc.c_str(), O_RDWR | O_CREAT, 0600);
        if (fd == -1) {
            perror("open");
            exit(1);
        }

        fmapvec_init(&vec, fd, sizeof(T));
    }

    void append(const T& elem) {
        fmapvec_append(&vec, &elem);
    }

    T* get(size_t idx) {
        return (T*)fmapvec_get(&vec, idx * sizeof(T));
    }

    size_t size() {
        return fmapvec_size(&vec);
    }

    void sync() {
        fmapvec_sync(&vec);
    }
};

class AbstractTable {
public:
    virtual void sync() = 0;
};

template<typename T>
class Table : public AbstractTable {
public:
    struct Entry {
        uint64_t timestamp;
        T value;
    };

private:
    std::shared_ptr<FileMappedVector<Entry>> data;
    uint64_t last_timestamp = 0;

public:
    Table(std::string loc) {
        data = std::make_shared<FileMappedVector<Entry>>(loc);
        if(data->size() > 0) {
            last_timestamp = data->get(data->size() - 1)->timestamp;
        }
    }

    void append(const uint64_t& timestamp, const T& value) {
        // timestamp must be strictly increasing
        if (data->size() > 0 && timestamp <= last_timestamp) {
            return;
        }

        last_timestamp = timestamp;
        data->append({timestamp, value});
    }

    Entry* get(size_t idx) {
        return data->get(idx);
    }

    Entry operator[](size_t idx) {
        return *get(idx);
    }

    size_t size() {
        return data->size();
    }

    size_t locate(uint64_t timestamp) {
        // tail optimized binary search
        // we know that the data is sorted by timestamp

        if(timestamp <= data->get(0)->timestamp) {
            return 0;
        }

        if(timestamp >= data->get(data->size() - 1)->timestamp) {
            return data->size() - 1;
        }

        return _locate(timestamp, 0, data->size() - 1);
    }

    std::vector<Entry> reduce(uint64_t t_start, uint64_t t_end, uint64_t dt) {
        // reduce by dropping data points
        size_t start = locate(t_start);
        size_t end = locate(t_end) + 1;
        end = std::min(end, size());

        std::vector<Entry> reduced;
        reduced.push_back(*data->get(start));

        uint64_t threshold = reduced.back().timestamp + dt;

        for (size_t i = start + 1; i < end; i++) {
            auto entry = data->get(i);

            if (entry->timestamp > threshold) {
                threshold = entry->timestamp + dt;
                reduced.push_back(*entry);
            }

            // next timestamp will be at least n indices away, so we can skip
            // checking those

            // assumption: rate of data doesn't get drastically faster
        }

        return reduced;
    }

    void sync() {
        data->sync();
    }

private:
    size_t _locate(uint64_t timestamp, size_t start, size_t end) {
        if (__builtin_expect(start == end, 0))
            return start;

        const size_t mid = (start + end) >> 1;
        const size_t nmid_l = (start + mid) >> 1;
        const size_t nmid_r = (mid + 1 + end) >> 1;

        __builtin_prefetch(data->get(nmid_l));
        __builtin_prefetch(data->get(nmid_r));

        const uint64_t mts = data->get(mid)->timestamp;
        const uint64_t prevts = data->get(mid - 1)->timestamp;

        if (mts >= timestamp && prevts < timestamp)
            return mid;

        if (mts < timestamp)
            return _locate(timestamp, mid + 1, end);

        return _locate(timestamp, start, mid);
    }
};

class Database {
    std::string loc;

    std::string table_loc(std::string name) {
        return loc + "/" + name;
    }

public:
    std::unordered_map<std::string, std::shared_ptr<AbstractTable>> tables;

    Database(std::string loc) : loc(loc) {
        // loc names a folder, create if doesn't exist

        if(!std::filesystem::exists(loc)) {
            std::filesystem::create_directory(loc);
        }
    }

    template<typename T>
    std::shared_ptr<Table<T>> get_table(std::string name) {
        if (tables.find(name) == tables.end()) {
            tables[name] = std::make_shared<Table<T>>(loc + "/" + name);
        }

        return std::dynamic_pointer_cast<Table<T>>(tables[name]);
    }

    void sync() {
        for (auto& [name, table] : tables) {
            table->sync();
        }
    }
};

} // namespace tsdb