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
}

#include <functional>
#include <unordered_map>
#include <memory>
#include <filesystem>

namespace tsdb {

static const size_t pagesize = getpagesize();
static const size_t MMAP_SIZE = 1ull << 40; // 1 TiB of addressable memory

typedef uint32_t sent_magic_t;

template<typename T>
class FileMappedVector {
    size_t _size;
    size_t capacity;

    const sent_magic_t sentinel_magic = 0xdeadbeef;

    struct sentinel {
        sent_magic_t magic;
        size_t used_size;
    };

    union elem {
        T data;
        sentinel sent;
    };

    int fd;
    elem* data;

    public:
    FileMappedVector(std::string loc) {
        size_t init_size = sizeof(elem) * 32;

        // create file if it doesn't exist
        fd = open(loc.c_str(), O_RDWR | O_CREAT, 0644);

        if (fd == -1) {
            std::cerr << "Error: could not open file " << loc << std::endl;
            return;
        }

        // get file size
        {
            struct stat st;
            fstat(fd, &st);

            if(st.st_size == 0) {
                // initialize file
                ftruncate(fd, init_size);

                // initialize sentinel
                elem init_sentinel;
                init_sentinel.sent.used_size = 0;
                init_sentinel.sent.magic = sentinel_magic;

                // write sentinel
                lseek(fd, (init_size - sizeof(elem)), SEEK_SET);
                write(fd, &init_sentinel, sizeof(elem));
                lseek(fd, 0, SEEK_SET);
            }
        }

        struct stat st;
        fstat(fd, &st);

        if (st.st_size % sizeof(elem) != 0) {
            std::cerr << "Error: file " << loc << " is invalid (unaligned size)" << std::endl;
            exit(1);
        }
        capacity = st.st_size / sizeof(elem);

        if (capacity == 0) {
            // invalid file
            std::cerr << "Error: file " << loc << " is invalid (empty file)" << std::endl;
            exit(1);
        }

        // map file to memory
        data = (elem*)mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Error: could not map file " << loc << " to memory" << std::endl;
            exit(1);
        }

        // most of this space doesn't exist, so we advise dontneed
        // madvise(data, MMAP_SIZE, MADV_DONTNEED);

        // last element is a sentinel, check magic
        elem* last = &data[capacity - 1];
        if (last->sent.magic != sentinel_magic) {
            std::cerr << "Error: file " << loc << " is invalid (invalid magic number)" << std::endl;
            exit(1);
        }

        _size = last->sent.used_size;

        // advise random
        // madvise(data, _size * sizeof(elem), MADV_RANDOM);
    };

    ~FileMappedVector() {
        msync(data, capacity * sizeof(elem), MS_SYNC);
        munmap(data, capacity * sizeof(elem));
        close(fd);
    };

    void append(const T& new_elem) {
        if (_size >= capacity - 1) {
            // resize
            size_t new_capacity = capacity * 2.71; // for some reason, e has best performance

            // expand file
            int ret = ftruncate(fd, new_capacity * sizeof(elem));
            if (ret == -1) {
                std::cerr << "Error: could not expand file" << std::endl;
                return;
            }

            capacity = new_capacity;

            // update sentinel
            elem* last = &data[capacity - 1];
            last->sent.magic = sentinel_magic;
            last->sent.used_size = _size;
        }

        data[_size].data = new_elem;
        _size++;

        // update sentinel
        elem* last = &data[capacity - 1];
        assert(last->sent.magic == sentinel_magic);
        last->sent.used_size = _size;
    };

    T* get(size_t i) {
        // don't bounds check
        // return &data[i].data;
        return (T*)(data + i);
    }

    T operator[](size_t i) {
        return data[i].data;
    };

    size_t size() {
        return _size;
    };

    void sync() {
        msync(data, capacity * sizeof(elem), MS_ASYNC);
    };
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
        reduced.push_back(data->operator[](start));

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