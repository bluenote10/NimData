#!/bin/bash

argfile=`readlink -f $1`

cd `dirname $0`

sbt package
JAR_FILE=`readlink -f target/scala-2.11/*_2.11-1.0.jar`
java -cp "${JAR_FILE}":$(cat target/streams/compile/dependencyClasspath/\$global/streams/export) nodomain.Benchmark $argfile
