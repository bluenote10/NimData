import macros

import strutils
import parseutils
import times

type
  ColKind* = enum
    StrCol,
    IntCol,
    FloatCol,
    DateCol
  Column* = object # TODO: this should get documented: https://forum.nim-lang.org/t/196
    name*: string
    case kind*: ColKind
    of StrCol:
      stripQuotes: bool
    of DateCol:
      format: string
    else:
      discard

proc strCol*(name: string): Column =
  Column(kind: StrCol, name: name)

proc intCol*(name: string): Column =
  Column(kind: IntCol, name: name)

proc floatCol*(name: string): Column =
  Column(kind: FloatCol, name: name)

proc dateCol*(name: string, format: string = "yyyy-MM-dd"): Column =
  Column(name: name, kind: DateCol, format: format)


template skipPastSep*(s: untyped, i: untyped, hitEnd: untyped, sep: char) =
  while s[i] != sep and i < s.len:
    i += 1
  if i == s.len:
    hitEnd = true
  else:
    i += 1

template skipOverWhitespace*(s: untyped, i: untyped) =
  while (s[i] == ' ' or s[i] == '\t') and i < s.len:
    i += 1


macro schemaType*(schema: static[openarray[Column]]): untyped =
  ## Creates a type corresponding to a given schema (the return
  ## type of the generated ``schemaParser`` proc).
  result = newNimNode(nnkTupleTy)
  for col in schema:
    # TODO: This can probably done using true types + type.getType.name
    let typ = case col.kind
      of StrCol: bindSym"string"
      of IntCol: bindSym"int64"
      of FloatCol: bindSym"float"
      of DateCol: bindSym"Time"
    result.add(
      newIdentDefs(name = newIdentNode(col.name), kind = typ)
    )


macro schemaParser*(schema: static[openarray[Column]], sep: static[char]): untyped =
  ## Creates a schema parser proc, which takes a ``string`` as input and
  ## returns a the parsing result as a tuple, with types corresponding to
  ## the given ``schema``
  # Adding `extraArgs: varargs[untyped]` doesn't seem to work :(

  # TODO: Why can't I just use:
  # var returnType = schemaType(schema)
  # /home/fabian/github/NimData/src/nimdata/schema_parser.nim(58, 30) Error: type mismatch: got (openarray[Column])
  # but expected one of:
  # macro schemaType[](schema: static[openArray[Column]]): untyped

  var returnType = newNimNode(nnkTupleTy)
  for col in schema:
    # TODO: This can probably done using true types + type.getType.name
    let typ = case col.kind
      of StrCol: bindSym"string"
      of IntCol: bindSym"int64"
      of FloatCol: bindSym"float"
      of DateCol: bindSym"Time"
    returnType.add(
      newIdentDefs(name = newIdentNode(col.name), kind = typ)
    )
  when defined(checkMacros):
    #echo returnType.treeRepr
    echo returnType.repr

  template fragmentSkipPastSep(sep: char) =
    skipPastSep(s, i, hitEnd, sep)

  template fragmentReadStr(field: untyped, sep: char) =
    ## read string
    copyFrom = i
    skipPastSep(s, i, hitEnd, sep)
    if not hitEnd:
      field = substr(s, copyFrom, i-2)
    else:
      field = substr(s, copyFrom, s.len)

  template fragmentReadDate(field: untyped, sep: char, format: string) =
    ## read string
    copyFrom = i
    skipPastSep(s, i, hitEnd, sep)
    let s =
      if not hitEnd:
        substr(s, copyFrom, i-2)
      else:
        substr(s, copyFrom, s.len)
    try:
      field = times.toTime(times.parse(s, format))
    except ValueError:
      # TODO: more systematic logging/error reporting system
      let e = getCurrentException()
      field = times.Time(0)
      echo "[WARNING] Failed to parse '" & s & "' as a time (" & e.msg & "). Setting value to " & times.`$`(field)

  template fragmentReadInt(field: untyped) =
    ## read int
    i += parseBiggestInt(s, field, start=i)

  template fragmentReadFloat(field: untyped) =
    ## read float
    skipOverWhitespace(s, i)
    i += parseBiggestFloat(s, field, start=i)

  template bodyHeader() {.dirty.} =
    var i = 0
    var hitEnd = false
    var copyFrom = 0

  var body = getAst(bodyHeader())

  for i, col in schema.pairs:

    let fieldExpr = newDotExpr(ident("result"), ident(col.name)) # the `result.columnBlah` expression
    let sepExpr = newLit(sep)

    var requiresAdvancePastSep = true

    case col.kind
    of StrCol:
      let call = getAst(fragmentReadStr(fieldExpr, sepExpr))
      body.add(call)
      # for a StrCol we don't need the call to fragmentSkipPastSep, because
      # the string extraction already advances past the separator
      requiresAdvancePastSep = false
    of DateCol:
      let call = getAst(fragmentReadDate(fieldExpr, sepExpr, col.format))
      body.add(call)
      # for a DateCol we don't need the call to fragmentSkipPastSep, because
      # the string extraction already advances past the separator
      requiresAdvancePastSep = false
    of IntCol:
      let call = getAst(fragmentReadInt(fieldExpr))
      body.add(call)
      requiresAdvancePastSep = true
    of FloatCol:
      let call = getAst(fragmentReadFloat(fieldExpr))
      body.add(call)
      requiresAdvancePastSep = true

    # If it is not the last column and dvancing past sep is required
    if requiresAdvancePastSep and i < schema.len - 1:
      let call = getAst(fragmentSkipPastSep(sepExpr))
      body.add(call)

  let params = [
    returnType,
    newIdentDefs(name = newIdentNode("s"), kind = newIdentNode("string"))
  ]
  result = newProc(params=params, body=body, procType=nnkLambda)
  when defined(checkMacros):
    #echo result.treerepr
    echo result.repr

