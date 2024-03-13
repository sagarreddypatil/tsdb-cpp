#include <iostream>

#include "database.hpp"
#include <cstdint>

struct SensorNetPoint {
    uint64_t counter;
    int64_t value;
};

void app(tsdb::Database<SensorNetPoint> db) {
    // create UDP socket
    // bind to port 1111
    // receive data

    
}

int main() {
    tsdb::Database<SensorNetPoint> db("db");
}