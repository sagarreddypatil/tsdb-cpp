#include <iostream>
#include <filesystem>
#include <vector>
#include <map>

#include "fmapvec.hpp"
#include "database.hpp"


int bruh() {
    struct datapoint {
        uint64_t c;
        int64_t v;
    };

    size_t npts = 500000000;
    Table<datapoint> vec("test.dat");

    // for(uint i = 0; i < (uint)npts; i++) {
    //     vec.append(i, {i, rand()});
    // }

    // generate a list of 1000 random numbers between 0 and npts
    std::vector<size_t> randoms;
    for(int i = 0; i < 5000000; i++) {
        randoms.push_back(rand() % npts);
    }

    volatile size_t index;

    // search for the random indices
    for(int idx : randoms) {
        index = vec.locate(idx);
    }

    return index;

    // sleep(10);


    // Database<datapoint> db("db");
    // db.make_table("test");
    // db.get_table("test")->append({1, 2});
}

int main() {
    bruh();
    return 0;
}