#!/usr/bin/env bash

make release
rm -rf db
./build/main

python dt.py