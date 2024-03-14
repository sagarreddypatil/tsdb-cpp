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

    auto reduced = table->reduce(0, npts, 10);

    // for (auto& entry : reduced) {
    //     std::cout << entry.timestamp << " " << entry.value.a << " " << entry.value.b << std::endl;
    // }

    // std::cout << reduced.size() << std::endl;

    return reduced.size();
}

int main() {
    // insertPoints();
    reducePoints();
    return 0;
}