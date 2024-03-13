#include <iostream>

#include "database.hpp"


int bruh() {
    struct datapoint {
        uint64_t c;
        int64_t v;
    };

    size_t npts = 500000000;
    tsdb::Table<datapoint> vec("test.dat");

    // for(uint i = 0; i < (uint)npts; i++) {
    //     vec.append(i, {i, rand()});
    // }

    // auto reduced = vec.reduce(0, npts, 10000);

    // print to csv
    std::cout << "time,counter,value" << "\n";
    for (size_t i = 0; i < npts; i++) {
        std::cout << vec[i].timestamp << "," << vec[i].value.c << "," << vec[i].value.v << "\n";
    }

    return npts;
}

int main() {
    bruh();
    return 0;
}