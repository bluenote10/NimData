#!/bin/bash

if [ -z "$1" ] ; then
  file=tests/all.nim
elif [ "$1" == "examples" ]; then
  file=examples/example_01.nim
elif [ "$1" == "benchmarks" ]; then
  file=benchmarks/NimData/basic_tests.nim
else
  echo "Unknown mode"
  exit 1
fi

echo "Compiling: $file"

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
  nim_repo=`which nim | xargs readlink -f | xargs dirname | xargs dirname`
  cd "$nim_repo"
  ./koch temp c "$fileAbs"
fi

