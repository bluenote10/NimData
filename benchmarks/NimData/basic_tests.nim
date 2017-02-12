
import future
import strutils
import nimdata
import times


template runTimed(name: string, body: untyped) =
  echo "Running: ", name, "..."
  let t1 = epochTime()
  body
  let t2 = epochTime()
  echo "Time: ", t2 - t1


proc runTests() =
  const schema = [
    col(FloatCol, "floatA"),
    col(FloatCol, "floatB"),
    col(IntCol, "intA"),
    col(IntCol, "intB"),
  ]

  runTimed("Pure iteration"):
    discard DF.fromFile("test_01.csv")
              .count()
  runTimed("Pure iteration"):
    discard DF.fromFile("test_01.csv")
              .count()
  runTimed("Pure iteration"):
    discard DF.fromFile("test_01.csv")
              .count()

  runTimed("With parsing"):
    discard DF.fromFile("test_01.csv")
              .map(schemaParser(schema, ','))
              .count()

  runTimed("With parsing + 1 dummy map"):
    discard DF.fromFile("test_01.csv")
              .map(schemaParser(schema, ','))
              .map(x => x)
              .count()

  runTimed("With parsing + 2 dummy map"):
    discard DF.fromFile("test_01.csv")
              .map(schemaParser(schema, ','))
              .map(x => x)
              .map(x => x)
              .count()

  runTimed("With parsing + 1 dummy filter"):
    discard DF.fromFile("test_01.csv")
              .map(schemaParser(schema, ','))
              .filter(x => true)
              .count()

  runTimed("With parsing + 2 dummy filter"):
    discard DF.fromFile("test_01.csv")
              .map(schemaParser(schema, ','))
              .filter(x => true)
              .filter(x => true)
              .count()


runTests()