import macros

import utils

macro projectTo*(t: typed, fields: varargs[untyped]): untyped =
  echo "t = ", t.repr
  let tType = t.getType               # returns the type as a NimNode
  let tKind = tType.kind              # note this is not the same as typeKind (bracket for a tuple)
  let tTypeKind = tType.typeKind

  echo "t.getType = ", t.getType.repr
  echo "t.getType.typeKind = ", tTypeKind
  echo "fields = ", fields.repr
  echo fields.treeRepr

  if not (tTypeKind in {ntyTuple, ntyObject}):
    error "projectTo expects a tuple or object, but received: " & t.repr &
          " which has typeKind " & tTypeKind.repr

  for field in fields:
    echo field.treeRepr
    if field.kind != nnkIdent:
      error "projectTo expects pure identifiers as varargs, but received: " &
            field.repr & " which is of kind " & $field.kind

    let tupleAccess = quote do: `t`.`field`
    echo tupleAccess.repr
    when compiles(tupleAccess):
      echo "compiles"
    else:
      echo "does NOT compile"

  result = quote do:
    1



UnitTestSuite("Tuples"):

  proc genTuple(): tuple[a: int, b: string] =
    result = (a: 1, b: "2")

  test "projectTo":

    let t = genTuple() # (name: "A", age: 99)
    echo t.projectTo(name)
    echo t.projectTo(name, ages)

    check true



