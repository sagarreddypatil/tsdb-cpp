#include <iostream>

#include "database.hpp"
#include <cstdint>
#include <chrono>
#include <thread>
#include <unistd.h>

#include <fstream>

struct DataPoint {
    int a;
    int b;
};

static const size_t npts = 100000;

volatile int counter = 0;

void insertPoints() {
    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    const auto start = std::chrono::high_resolution_clock::now();

    // wait until user presses enter
    // std::cout << "Press enter to start inserting points" << std::endl;
    // std::cin.get();
    for (int i = 0; i < (int)npts; i++) {
        auto time = std::chrono::high_resolution_clock::now();
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(time - start).count();
        // uint64_t timestamp = i;

        table->append(timestamp, {i * 2, i * 3});

        // for(volatile int j = 0; j < 4500;) {
        //     int k = j + 1;
        //     j = k;
        // }
    }
    // exit(0);
    std::cout << "Inserted " << npts << " points" << std::endl;

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

    size_t size = table->size();
    for (size_t i = 0; i < size; i++) {
        auto entry = table->get(i);
        file << entry->timestamp << "," << entry->value.a << "," << entry->value.b << "\n";
    }
}

int main() {
    // print my pid
    std::cout << "PID: " << getpid() << std::endl;

    insertPoints();
    toCSV();
    // reducePoints();
    // benchLocate();
    // benchRead();
    return 0;
}
