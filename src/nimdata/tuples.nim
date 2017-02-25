import macros
import typetraits

import utils


macro projectTo*(t: typed, fields: varargs[untyped]): untyped =

  if fields.len == 0:
    error "At least one field required to project to."

  let tType = t.getType               # returns the type as a NimNode
  let tTypeKind = tType.typeKind

  if not (tTypeKind in {ntyTuple, ntyObject}):
    error "projectTo expects a tuple or object, but received: " & t.repr &
          " which has typeKind " & tTypeKind.repr

  # extract fields present in given tuple
  var existingFieldNames = newSeq[string]() # actually we should switch to hashset...
  let tTypeInst = t.getTypeInst
  for child in tTypeInst.children:
    if child.kind != nnkIdentDefs:
      error "projectTo expects a tuple or object, consisting of nnkIdentDefs children."
    else:
      let field = child[0] # first child of IdentDef is a Sym corresponding to field name
      existingFieldNames.add($field)

  # iterate over fields to project to
  result = newPar()
  for field in fields:
    if field.kind != nnkIdent:
      error "projectTo expects pure identifiers as varargs, but received: " &
            field.repr & " which is of kind " & $field.kind

    if $field in existingFieldNames:
      # create the `t.field` expression
      let fieldExpr = newDotExpr(t, field)
      # and add `field: t.field` to the Par experession
      result.add(
        newColonExpr(newIdentNode($field), fieldExpr)
      )
    else:
      error "Can't project to " & $field & " (does not exist in source tuple)"



UnitTestSuite("Tuples"):

  proc genTupleFromProc(): tuple[a: int, b: string] =
    result = (a: 1, b: "2")

  test "projectTo":

    block:
      let t = (name: "A", age: 99)
      check: t.projectTo(name, age) == (name: "A", age: 99)
      check: t.projectTo(name) == (name: "A")
      check: t.projectTo(age) == (age: 99)

    block:
      check: (name: "A", age: 99).projectTo(name, age) == (name: "A", age: 99)
      check: (name: "A", age: 99).projectTo(name) == (name: "A")
      check: (name: "A", age: 99).projectTo(age) == (age: 99)

    block:
      let t = genTupleFromProc()
      check: t.projectTo(a, b) == (a: 1, b: "2")
      check: t.projectTo(a) == (a: 1)
      check: t.projectTo(b) == (b: "2")

    block:
      check: genTupleFromProc().projectTo(a, b) == (a: 1, b: "2")
      check: genTupleFromProc().projectTo(a) == (a: 1)
      check: genTupleFromProc().projectTo(b) == (b: "2")

    block:
      let t = (name: "A", age: 99, height: 200.0)
      check: t.projectTo(name, age, height).projectTo(name, age, height) == t
      check: t.projectTo(name, age).projectTo(age) == (age: 99)

    block:
      let t = (name: "A", age: 99, height: 200.0)
      check: notCompiles: t.projectTo()
      check: notCompiles: t.projectTo(nonExisting)
      check: notCompiles: t.projectTo(asdf asdf)
      check: notCompiles: t.projectTo(name, age, name)
      check: notCompiles: t.projectTo(name, age).projectTo(height)

