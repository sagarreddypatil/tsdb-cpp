#!/bin/bash

make
time systemd-run --scope -p MemoryMax=50M --user ./build/main
