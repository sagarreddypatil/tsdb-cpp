#include <iostream>

#include "database.hpp"
#include <cstdint>
#include <chrono>

#include <fstream>

struct DataPoint {
    int a;
    int b;
};

static const size_t npts = 100000;

void insertPoints() {
    std::cout << "Inserting " << npts << " points" << std::endl;

    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    const auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < (int)npts; i++) {
        auto time = std::chrono::high_resolution_clock::now();
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(time - start).count();
        assert(timestamp != 0);
        // uint64_t timestamp = i;

        table->append(timestamp, {i * 2, i * 3});
    }

    db.sync();
}

size_t reducePoints() {
    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    auto reduced = table->reduce(0, npts, 0);

    // for (auto& entry : reduced) {
    //     std::cout << entry.timestamp << " " << entry.value.a << " " << entry.value.b << std::endl;
    // }

    std::cout << reduced.size() << std::endl;

    return reduced.size();
}

size_t benchLocate() {
    static volatile size_t p;

    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    std::vector<uint64_t> rand_ts;
    for (int i = 0; i < 1000000; i++)
        rand_ts.push_back(rand() % npts);

    for (auto ts : rand_ts) {
        p = table->locate(ts);
    }

    return p;
}

tsdb::Table<DataPoint>::Entry entry;

size_t benchRead() {
    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    for(int i = 0; i < 50; i++) {
        for (size_t i = 0; i < npts; i++) {
            entry = *(table->get(i));
        }
    }

    return npts;
}

void toCSV() {
    std::cout << "Writing to CSV" << std::endl;

    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    std::ofstream file("data.csv");
    file << "timestamp,a,b\n";

    for (size_t i = 0; i < table->size(); i++) {
        auto entry = table->get(i);
        file << entry->timestamp << "," << entry->value.a << "," << entry->value.b << "\n";
    }
}

int main() {
    insertPoints();
    toCSV();
    // reducePoints();
    // benchLocate();
    // benchRead();
    return 0;
}