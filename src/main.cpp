#include <iostream>

#include "database.hpp"
#include <cstdint>

struct DataPoint {
    int a;
    int b;
};

static const size_t npts = 100000000;

void insertPoints() {
    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    for (int i = 0; i < (int)npts; i++) {
        table->append(i, {i * 2, i * 3});
    }

    db.sync();
}

size_t reducePoints() {
    tsdb::Database db("db");
    auto table = db.get_table<DataPoint>("mytable");

    auto reduced = table->reduce(0, npts, 100);

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

int main() {
    // insertPoints();
    // reducePoints();
    benchLocate();
    return 0;
}