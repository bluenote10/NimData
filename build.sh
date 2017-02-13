#!/bin/bash

file=tests/all.nim
#file=examples/example_01.nim
#file=benchmarks/NimData/basic_tests.nim

fileAbs=`readlink -m $file`
traceback=false

optargs=""
#optargs="-d:checkMacros"

cd `dirname $0`
mkdir -p bin
nim c -o:./bin/tests --parallelBuild:1 -d:testNimData -d:release $optargs $file

compiler_exit=$?

echo "Compiler exit: $compiler_exit"

if [ "$compiler_exit" -eq 0 ]; then  # compile success
  ./bin/tests
  exit $?
fi

if [ "$traceback" = true ] ; then
  echo -e "\nRunning ./koch temp c $fileAbs"
  cd ~/bin/nim-repo
  ./koch temp c `readlink -m $fileAbs`
fi

