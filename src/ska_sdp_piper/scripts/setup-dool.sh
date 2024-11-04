#! /usr/bin/env bash

set -e

wget https://github.com/scottchiefbaker/dool/archive/refs/tags/v1.3.3.tar.gz -O dool.tar.gz

mkdir -p $1

tar -xvf dool.tar.gz -C $1 --strip-components=1

rm -rf dool.tar.gz

