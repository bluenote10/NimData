import macros
import sets
import tables

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
  var tupleFieldsToRemove = initHashSet[string]()
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


macro addFields*(t: typed, fields: varargs[untyped]): untyped =
  ## Returns a new tuple expression, containing all fields of
  ## the given tuple, plus new fields as specified in the varargs.
  ## New field expressions are specified as colon expressions, i.e.:
  ## .. code-block:: nim
  ##   let t = (x: 1.0, y: 1.0)
  ##   let tExtended = addFields(t, length: sqrt(t.x*t.x + t.y*t.y))
  ##
  ## Notes:
  ## - The expression must use the fully qualified name (like ``t.x``),
  ##   not just the field names (``x``).
  ## - The syntax ``t.addFields(length: sqrt(t.x*t.x + t.y*t.y)`` is currently
  ##   not yet supported by the compiler.

  # echo fields.treeRepr
  # echo fields.len
  # echo fields.repr
  if fields.len == 0:
    error "addFields expects at least one field."

  # check type of t
  var tTypeImpl = t.getTypeImpl
  case tTypeImpl.typeKind:
  of ntyTuple:
    discard
  of ntyObject:
    tTypeImpl = tTypeImpl[2]
  else:
    error "addFields expects a tuple or object, but received: " & t.repr &
          " which has typeKind " & tTypeImpl.typeKind.repr

  # extract fields present in given tuple
  var tupleFields = newSeq[string]()
  for child in tTypeImpl.children:
    if child.kind != nnkIdentDefs:
      error "addFields expects a tuple or object, consisting of nnkIdentDefs children."
    else:
      let field = child[0] # first child of IdentDef is a Sym corresponding to field name
      tupleFields.add($field)

  # add all already existing fields first
  result = newPar()
  for field in tupleFields:
    # create the `t.field` expression
    let fieldExpr = newDotExpr(t, ident(field))
    # and add `field: t.field` to the Par experession
    result.add(
      newColonExpr(newIdentNode($field), fieldExpr)
    )

  # add new fields
  for field in fields:
    if field.kind != nnkExprColonExpr:
      error "addFields expects varargs of kind nnkExprColonExpr, but received: " &
            field.repr & " which is of kind " & $field.kind
    else:
      let fieldName = field[0]
      let fieldExpr = field[1]
      result.add(
        newColonExpr(fieldName, fieldExpr)
      )


macro addField*(t: typed, field: untyped): untyped =
  ## Returns a new tuple expression, containing all fields of
  ## the given tuple, plus a new field as specified in ``field``.
  ## New field expressions are specified as colon expressions, i.e.:
  ## .. code-block:: nim
  ##   let t = (x: 1.0, y: 1.0)
  ##   let tExtended = addField(t, length: sqrt(t.x*t.x + t.y*t.y))
  ##
  ## Notes:
  ## - The expression must use the fully qualified name (like ``t.x``),
  ##   not just the field names (``x``).
  ## - The syntax ``t.addFields(length: sqrt(t.x*t.x + t.y*t.y)`` is currently
  ##   not yet supported by the compiler.

  # echo fields.treeRepr
  # echo fields.len
  # echo fields.repr

  # check type of t
  var tTypeImpl = t.getTypeImpl
  case tTypeImpl.typeKind:
  of ntyTuple:
    discard
  of ntyObject:
    tTypeImpl = tTypeImpl[2]
  else:
    error "addField expects a tuple or object, but received: " & t.repr &
          " which has typeKind " & tTypeImpl.typeKind.repr

  # extract fields present in given tuple
  var tupleFields = newSeq[string]()
  for child in tTypeImpl.children:
    if child.kind != nnkIdentDefs:
      error "addField expects a tuple or object, consisting of nnkIdentDefs children."
    else:
      let field = child[0] # first child of IdentDef is a Sym corresponding to field name
      tupleFields.add($field)

  # add all already existing fields first
  result = newPar()
  for field in tupleFields:
    # create the `t.field` expression
    let fieldExpr = newDotExpr(t, ident(field))
    # and add `field: t.field` to the Par experession
    result.add(
      newColonExpr(newIdentNode($field), fieldExpr)
    )

  # add new field
  if field.kind != nnkExprColonExpr:
    error "addField expects varargs of kind nnkExprColonExpr, but received: " &
          field.repr & " which is of kind " & $field.kind
  else:
    let fieldName = field[0]
    let fieldExpr = field[1]
    result.add(
      newColonExpr(fieldName, fieldExpr)
    )


proc extractFields(n: NimNode): OrderedTable[string, NimNode] =
  # extract fields present in given tuple
  result = initOrderedTable[string, NimNode]()
  # echo n.treeRepr
  for child in n.children:
    if child.kind != nnkIdentDefs:
      error "extractFields expects a tuple or object, consisting of nnkIdentDefs children."
    else:
      let fname = child[0]
      let ftype = child[1]
      result[$fname] = ftype

macro determineType*(ta, tb: typed, on: static[openarray[string]]): untyped =
  # getTypeImpl returns something like:
  # BracketExpr
  #   Sym "typeDesc"
  #   TupleTy
  # So we need child at index 1 to get the tuple/object type
  let fieldsA = extractFields(ta.getTypeImpl[1])
  let fieldsB = extractFields(tb.getTypeImpl[1])
  echo "Fields in A:"
  for key, val in fieldsA:
    echo "    ", key, " -> ", val.repr
  echo "Fields in B:"
  for key, val in fieldsB:
    echo "    ", key, " -> ", val.repr

  result = newNimNode(nnkTupleTy)
  for field in on:
    if not (field in fieldsA):
      error "Operand A does not have required field: " & field
    elif not (field in fieldsB):
      error "Operand B does not have required field: " & field
    elif fieldsA[field] != fieldsB[field]:
      error "Operands do not have same type\n" &
            "Type of field '" & field & "' in operand A: " & fieldsA[field].repr & "\n" &
            "Type of field '" & field & "' in operand B: " & fieldsB[field].repr
    else:
      result.add(
        newIdentDefs(name=ident(field), kind=fieldsA[field])
      )

  for field, ftype in fieldsA:
    if not (field in on):
      if field in fieldsB:
        result.add(
          newIdentDefs(name=ident(field & "A"), kind=ftype)
        )
      else:
        result.add(
          newIdentDefs(name=ident(field), kind=ftype)
        )

  for field, ftype in fieldsB:
    if not (field in on):
      if field in fieldsA:
        result.add(
          newIdentDefs(name=ident(field & "B"), kind=ftype)
        )
      else:
        result.add(
          newIdentDefs(name=ident(field), kind=ftype)
        )

macro mergeTuple*(a: tuple, b: tuple, on: static[openarray[string]]): untyped =
  ## Merges the fields of a tuple ``a`` and ``b`` into a new named tuple.
  ## The field names are merged using the logic:
  ## - If a field occurs either in ``a`` or ``b``, the field is merged keeping
  ##   its original name.
  ## - If tuple ``a`` and ``b`` both have a field with the same name ``fieldName``,
  ##   they will be renamed to ``fieldNameA`` and ``fieldNameB`` (if such
  ##   a field already exists, there would be a compiler error).
  ## - If ``a.fieldName`` and ``b.fieldName`` are referring to the same
  ##   information (e.g. if ``fieldName`` is part of a join condition),
  ##   the field duplication can be avoided by specifying the field in
  ##   the on clause. In this case the result would only contain the
  ##   field ``fieldName``, and the value is taked from tuple ``a``.

  let fieldsA = extractFields(a.getTypeImpl)
  let fieldsB = extractFields(b.getTypeImpl)

  result = newNimNode(nnkPar)
  for field in on:
    if not (field in fieldsA):
      error "Operand A does not have required field: " & field
    elif not (field in fieldsB):
      error "Operand B does not have required field: " & field
    elif fieldsA[field] != fieldsB[field]:
      error "Operands do not have same type\n" &
            "Type of field '" & field & "' in operand A: " & fieldsA[field].repr & "\n" &
            "Type of field '" & field & "' in operand B: " & fieldsB[field].repr
    else:
      let dotExpr = newDotExpr(a, ident(field))
      result.add(
        newColonExpr(ident(field), dotExpr)
      )

  for field, ftype in fieldsA:
    if not (field in on):
      var fieldName = field
      if field in fieldsB:
        fieldName &= "A"
      let dotExpr = newDotExpr(a, ident(field))
      result.add(
        newColonExpr(ident(fieldName), dotExpr)
      )

  for field, ftype in fieldsB:
    if not (field in on):
      var fieldName = field
      if field in fieldsA:
        fieldName &= "B"
      let dotExpr = newDotExpr(b, ident(field))
      result.add(
        newColonExpr(ident(fieldName), dotExpr)
      )


macro tupleMatches*(a: tuple, b: tuple, on: static[openarray[string]]): untyped =
  let fieldsA = extractFields(a.getTypeImpl)
  let fieldsB = extractFields(b.getTypeImpl)
  result = newNimNode(nnkPar)
  for field in on:
    if not (field in fieldsA):
      error "Operand A does not have required field: " & field
    elif not (field in fieldsB):
      error "Operand B does not have required field: " & field
    else:
      let exprA = newDotExpr(a, ident(field))
      let exprB = newDotExpr(b, ident(field))
      result.add(
        infix(exprA, "==", exprB)
      )
  # echo result.repr



when isMainModule:
  import unittest
  import math

  suite("Tuples"):

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


    test "addField":

      let t = (x: 1.0, y: 1.0)
      let tExtended = addField(t, length: sqrt(t.x*t.x + t.y*t.y))
      check tExtended == (x: 1.0, y: 1.0, length: sqrt(2.0))


    test "addFields":

      let t = (x: 1.0, y: 1.0)
      #let tExtended = t.addFields(length: x*x + y*y)
      let tExtended = addFields(t, length1: sqrt(t.x*t.x + t.y*t.y), length2: sqrt(t.x*t.x + t.y*t.y))
      check tExtended == (x: 1.0, y: 1.0, length1: sqrt(2.0), length2: sqrt(2.0))

