import macros

import strutils

type
  ColKind* = enum
    IntCol,
    FloatCol
  Column* = object
    kind*: ColKind
    name*: string

proc col*(kind: ColKind, name: string): Column =
  Column(kind: kind, name: name)


macro schemaParser*(schema: static[openarray[Column]]): untyped =
  #let returnType = newBracketExpr(newIdentNode(!"tuple"), newIdentNode(!"y"))
  #let returnType = newNimNode(nnkBracketExpr).add(ident("tuple"), newColonExpr(ident("x"), ident("int")))
  var returnType = newNimNode(nnkTupleTy)
  for col in schema:
    # TODO: This can probably done using true types + type.getType.name
    let typ = case col.kind
      of IntCol: "int"
      of FloatCol: "float"
    returnType.add(
      newIdentDefs(name = newIdentNode(col.name), kind = ident(typ))
    )
  when defined(checkMacros):
    echo returnType.treeRepr
    echo returnType.repr

  when defined(checkMacros):
    let test = quote do:
      let example2 = proc (s: string): tuple[A: int, B: int, C: float] =
        let fields = s.split(";")
        result.A = parseInt(fields[0])
        result.B = parseInt(fields[1])
        result.C = parseFloat(fields[2])
    echo test.treerepr
    echo test.repr

  let fieldsIdent = ident("fields")
  let expectedFields = newIntLitNode(schema.len)
  var body = quote do:
    let `fieldsIdent` = s.split(";")
    if `fieldsIdent`.len != `expectedFields`:
      raise newException(IOError, "Unexpected number of fields")
  for i, col in schema.pairs:
    let parserFunc = case col.kind
      of IntCol: bindSym("parseInt")
      of FloatCol: bindSym("parseFloat")
    let ass_lhs = newDotExpr(ident("result"), ident(col.name))
    let ass_rhs = newCall(parserFunc, newNimNode(nnkBracketExpr).add(ident("fields"), newIntLitNode(i)))
    body.add(newAssignment(ass_lhs, ass_rhs))
  when defined(checkMacros):
    echo body.treeRepr
    echo body.repr

  let params = [
    returnType,
    newIdentDefs(name = newIdentNode("s"), kind = newIdentNode("string"))
  ]
  result = newProc(params=params, body=body, procType=nnkLambda)
  when defined(checkMacros):
    echo result.treerepr
    echo result.repr
