#!/bin/bash

file=src/nimdata.nim

cd `dirname $0`
mkdir -p docs
nim doc2 --project --docSeeSrcUrl:https://github.com/bluenote10/NimData/blob/master -o:./docs/ $file # -o:./bin/tests

