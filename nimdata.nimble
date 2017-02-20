
# Package

version       = "0.1.0"
author        = "Fabian Keller"
description   = "DataFrame API enabling fast out-of-core data analytics"
license       = "MIT"

srcDir = "src"

# Dependencies

requires "nim >= 0.16.0"
requires "zip >= 0.1.1"

task test, "Runs unit tests":
  # TODO: How can I ensure nimble installs deps before running this?
  mkdir("bin")
  var cc = getEnv("CC")
  if cc == "":
    cc = "gcc"
  exec "nim c --cc:" & cc & " --verbosity:0 -r -d:testNimData -o:bin/tests tests/all.nim"

