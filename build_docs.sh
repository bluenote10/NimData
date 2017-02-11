#!/bin/bash

file=src/nimdata.nim

fileAbs=`readlink -m $file`
traceback=false

cd `dirname $0`
mkdir -p docs
nim doc2 --project -o:./docs/ $file # -o:./bin/tests

