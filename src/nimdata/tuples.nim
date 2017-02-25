import macros
import sets

import utils


macro projectTo*(t: typed, fields: varargs[untyped]): untyped =
  ## Allows to project a given tuple to a subset of its fields,
  ## by keeping only the specified fields.

  if fields.len == 0:
    error "At least one field required to project to."

  # check type of t
  var tTypeImpl = t.getTypeImpl
  # echo tTypeImpl.len
  # echo tTypeImpl.kind
  # echo tTypeImpl.typeKind
  # echo tTypeImpl.treeRepr
  case tTypeImpl.typeKind:
  of ntyTuple:
    # For a tuple the IdentDefs are top level, no need to descent
    discard
  of ntyObject:
    # For an object the children are (https://forum.nim-lang.org/t/2483#15391):
    # - pragmas (=> typically Empty)
    # - parent (=> typically Empty)
    # - nnkRecList, which contains the IdentDefs we are looking for
    tTypeImpl = tTypeImpl[2]
  else:
    error "projectTo expects a tuple or object, but received: " & t.repr &
          " which has typeKind " & tTypeImpl.typeKind.repr

  # extract fields present in given tuple
  var tupleFields = initOrderedSet[string]()
  for child in tTypeImpl.children:
    if child.kind != nnkIdentDefs:
      error "projectTo expects a tuple or object, consisting of nnkIdentDefs children."
    else:
      let field = child[0] # first child of IdentDef is a Sym corresponding to field name
      tupleFields.incl($field)

  # iterate over fields to project to
  result = newPar()
  for field in fields:
    if field.kind != nnkIdent:
      error "projectTo expects pure identifiers as varargs, but received: " &
            field.repr & " which is of kind " & $field.kind

    if $field in tupleFields:
      # create the `t.field` expression
      let fieldExpr = newDotExpr(t, field)
      # and add `field: t.field` to the Par experession
      result.add(
        newColonExpr(newIdentNode($field), fieldExpr)
      )
    else:
      error "Can't project to " & $field & " (does not exist in source tuple)"


macro projectAway*(t: typed, fields: varargs[untyped]): untyped =
  ## Allows to project a given tuple to a subset of its fields,
  ## by removing the specified fields.

  # check type of t
  var tTypeImpl = t.getTypeImpl
  # echo tTypeImpl.len
  # echo tTypeImpl.kind
  # echo tTypeImpl.typeKind
  # echo tTypeImpl.treeRepr
  case tTypeImpl.typeKind:
  of ntyTuple:
    # For a tuple the IdentDefs are top level, no need to descent
    discard
  of ntyObject:
    # For an object the children are (https://forum.nim-lang.org/t/2483#15391):
    # - pragmas (=> typically Empty)
    # - parent (=> typically Empty)
    # - nnkRecList, which contains the IdentDefs we are looking for
    tTypeImpl = tTypeImpl[2]
  else:
    error "projectAway expects a tuple or object, but received: " & t.repr &
          " which has typeKind " & tTypeImpl.typeKind.repr

  # extract fields present in given tuple
  var tupleFields = initOrderedSet[string]()
  for child in tTypeImpl.children:
    if child.kind != nnkIdentDefs:
      error "projectAway expects a tuple or object, consisting of nnkIdentDefs children."
    else:
      let field = child[0] # first child of IdentDef is a Sym corresponding to field name
      tupleFields.incl($field)

  # iterate over user specified fields to verify their correctness
  # and remove from tupleFields
  var tupleFieldsToRemove = initSet[string]()
  for field in fields:
    if field.kind != nnkIdent:
      error "projectAway expects pure identifiers as varargs, but received: " &
            field.repr & " which is of kind " & $field.kind
    if $field in tupleFields:
      tupleFieldsToRemove.incl($field)
    else:
      error "Can't remove field " & $field & " (does not exist in source tuple)"

  # verify that any fields are left
  if tupleFields.len == tupleFieldsToRemove.len:
    error "All fields are projected away."

  # iterate over fields to project to
  result = newPar()
  for field in tupleFields:
    if not (field in tupleFieldsToRemove):
      # create the `t.field` expression
      let fieldExpr = newDotExpr(t, ident(field))
      # and add `field: t.field` to the Par experession
      result.add(
        newColonExpr(newIdentNode($field), fieldExpr)
      )



UnitTestSuite("Tuples"):

  proc genTupleFromProc(): tuple[a: int, b: string] =
    result = (a: 1, b: "2")

  test "projectTo":

    block:
      let t = (name: "A", age: 99)
      check: t.projectTo(name, age) == (name: "A", age: 99)
      check: t.projectTo(age, name) == (age: 99, name: "A")
      check: t.projectTo(name) == (name: "A")
      check: t.projectTo(age) == (age: 99)

      check: (name: "A", age: 99).projectTo(name, age) == (name: "A", age: 99)
      check: (name: "A", age: 99).projectTo(age, name) == (age: 99, name: "A")
      check: (name: "A", age: 99).projectTo(name) == (name: "A")
      check: (name: "A", age: 99).projectTo(age) == (age: 99)

    block:
      let t = genTupleFromProc()
      check: t.projectTo(a, b) == (a: 1, b: "2")
      check: t.projectTo(a) == (a: 1)
      check: t.projectTo(b) == (b: "2")

      check: genTupleFromProc().projectTo(a, b) == (a: 1, b: "2")
      check: genTupleFromProc().projectTo(a) == (a: 1)
      check: genTupleFromProc().projectTo(b) == (b: "2")

    block:
      let t = (name: "A", age: 99, height: 200.0)
      check: t.projectTo(name, age, height).projectTo(name, age, height) == t
      check: t.projectTo(name, age).projectTo(age) == (age: 99)

    block:
      type
        TestObj = object
          x: int
          y: int
      check: TestObj(x: 1, y: 2).projectTo(x, y) == (x: 1, y: 2)
      check: TestObj(x: 1, y: 2).projectTo(x) == (x: 1)
      check: TestObj(x: 1, y: 2).projectTo(y) == (y: 2)

    block:
      let t = (name: "A", age: 99, height: 200.0)
      check: notCompiles: t.projectTo()
      check: notCompiles: t.projectTo(nonExisting)
      check: notCompiles: t.projectTo(asdf asdf)
      check: notCompiles: t.projectTo(name, age, name)
      check: notCompiles: t.projectTo(name, age).projectTo(height)

  test "projectAway":

    block:
      let t = (name: "A", age: 99)
      check: t.projectAway(name) == (age: 99)
      check: t.projectAway(age) == (name: "A")
      check: t.projectAway() == (name: "A", age: 99)

    block:
      check: (name: "A", age: 99).projectAway(name) == (age: 99)
      check: (name: "A", age: 99).projectAway(age) == (name: "A")
      check: (name: "A", age: 99).projectAway() == (name: "A", age: 99)

    block:
      type
        TestObj = object
          x: int
          y: int
      check: TestObj(x: 1, y: 2).projectAway(x) == (y: 2)
      check: TestObj(x: 1, y: 2).projectAway(y) == (x: 1)
      check: TestObj(x: 1, y: 2).projectAway() == (x: 1, y: 2)

    block:
      let t = (name: "A", age: 99, height: 200.0)
      check: notCompiles: t.projectAway(name, age, height)
      check: notCompiles: t.projectAway(nameDoesNotExist)
      check: notCompiles: t.projectAway(name).projectAway(name)
