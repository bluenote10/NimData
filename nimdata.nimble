
# Package
version       = "0.1.0"
author        = "Fabian Keller"
description   = "DataFrame API enabling fast out-of-core data analytics"
license       = "MIT"

srcDir = "src"
binDir = "bin"

# Dependencies
requires "nim >= 0.16.0"
requires "zip >= 0.1.1"
requires "plotly"


import strutils

proc `/`(a, b: string): string = a & "/" & b

iterator iterateSourceFiles(dir: string): string =
  # This is currently a bit ugly. Why isn't there a
  # recursive listFiles? Would also be nice if we
  # could sort the files by name.
  for f in listFiles(dir / "src"):
    if f.endsWith(".nim"):
      yield f
  for f in listFiles(dir / "src/nimdata"):
    if f.endsWith(".nim"):
      yield f
  for f in listFiles(dir / "examples"):
    if f.endsWith(".nim"):
      yield f

proc runTest(file: string) =
  echo "\n *** Running tests in: ", file
  mkdir("bin")
  var cc = getEnv("CC")
  if cc == "":
    cc = "gcc"
  exec "nim c --cc:" & cc & " --verbosity:0 -r -d:testNimData -o:bin/tests " & file

task tests, "Runs unit tests":
  # TODO: How can I ensure nimble installs deps before running this?
  withDir(thisDir()):
    runTest("tests/all.nim")
    runTest("src/nimdata/tuples.nim")
    runTest("examples/example_01.nim")
    runTest("examples/example_02.nim")

    # This does currently not make sense, due to include only source files:
    # for f in iterateSourceFiles(thisDir()):
    #   runTest(f)
