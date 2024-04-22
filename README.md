# TSDB

A fairly straightforward "database" for storing time series data.

## Operating System Support
- Built and tested on Linux
- macOS should work, untested
- other UNIXes should work
 - FreeBSD has a warning about how `mmap` is used in this project
   as it will cause significant disk fragmentation, and thus slow
   read back times.

## Dependencies
- None

## Usage

It's a header only library, just copy `include/database.h` into your project.

CMake is only for building `main.cpp`, which contains some simple examples
and imprecise benchmarks.

Example usage:
```cpp
// define a data point
struct DataPoint {
    int a;
    int b;
};

tsdb::Database db("db"); // creates a folder called "db"
auto table = db.get_table<DataPoint>("mytable"); // creates file "db/mytable"

static const size_t npts = 100000000;

// insert points
// timestamps are uint64_t
for (int i = 0; i < (int)npts; i++) {
    auto timestamp = i; // whatever units you want
                        // I am partial to microseconds since UNIX epoch
    auto x = i * 2;
    auto y = i * 3;
    
    // timestamps must be in strictly increasing order
    // points are dropped WITHOUT WARNING if you insert an invalid timestamp
    table->append(timestamp, {x, y});
}

auto dt = 10; // 10 time units
auto reduced = table->reduce(0, npts, 10); // sample a point every 10 time units

// get the point closest to timestamp 123, binary search
auto p = table->locate(123);

// read back points, without reduction
tsdb::Table<DataPoint>::Entry entry;
for(int i = 0; i < 50; i++) {
    for (size_t i = 0; i < npts; i++) {
        entry = *(table->get(i));
    }
}
```

## Why should you use it?

It's really fast, that's basically about it.

There are no dependencies, and there are no cool features. Litearlly
all it does is mmap a massive file and write to it, expanding the file
as needed. It is not storage space efficient, the written file can be read
directly into memory.