import macros

import strutils
import parseutils
import times

type
  ColKind* = enum
    StrCol,
    IntCol,
    Int8Col,
    Int16Col,
    Int32Col,
    UIntCol,
    UInt8Col,
    UInt16Col,
    UInt32Col,
    FloatCol,
    DateCol
  ColIntBase* = enum
    baseBin,
    baseOct,
    baseDec,
    baseHex
  Column* = object # TODO: this should get documented: https://forum.nim-lang.org/t/196
    name*: string
    case kind*: ColKind
    of IntCol .. UInt32Col:
      base: ColIntBase
    of StrCol:
      stripQuotes: bool
    of DateCol:
      format: string
    else:
      discard

proc strCol*(name: string, stripQuotes: bool = false): Column =
  Column(kind: StrCol, name: name, stripQuotes: stripQuotes)

proc intCol*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: IntCol, name: name, base: base)

proc int8Col*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: Int8Col, name: name, base: base)

proc int16Col*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: Int16Col, name: name, base: base)

proc int32Col*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: Int32Col, name: name, base: base)

proc uintCol*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: UIntCol, name: name, base: base)

proc uint8Col*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: UInt8Col, name: name, base: base)

proc uint16Col*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: UInt16Col, name: name, base: base)

proc uint32Col*(name: string, base: ColIntBase = baseDec): Column =
  Column(kind: UInt32Col, name: name, base: base)

proc floatCol*(name: string): Column =
  Column(kind: FloatCol, name: name)

proc dateCol*(name: string, format: string = "yyyy-MM-dd"): Column =
  Column(name: name, kind: DateCol, format: format)

proc parseBin[T: SomeSignedInt](s: string, number: var T, start = 0): int  {.
  noSideEffect.} =
  var i = start
  var foundDigit = false
  if s[i] == '0' and (s[i+1] == 'b' or s[i+1] == 'B'): inc(i, 2)
  while true:
    case s[i]
    of '_': discard
    of '0'..'1':
      number = number shl 1 or (ord(s[i]) - ord('0'))
      foundDigit = true
    else: break
    inc(i)
  if foundDigit: result = i-start

proc parseOct[T: SomeSignedInt](s: string, number: var T, start = 0): int  {.
  noSideEffect.} =
  var i = start
  var foundDigit = false
  if s[i] == '0' and (s[i+1] == 'o' or s[i+1] == 'O'): inc(i, 2)
  while true:
    case s[i]
    of '_': discard
    of '0'..'7':
      number = number shl 3 or (ord(s[i]) - ord('0'))
      foundDigit = true
    else: break
    inc(i)
  if foundDigit: result = i-start

proc parseHex[T: SomeSignedInt](s: string, number: var T, start = 0; maxLen = 0): int {.
  noSideEffect.}  =
  var i = start
  var foundDigit = false
  if s[i] == '0' and (s[i+1] == 'x' or s[i+1] == 'X'): inc(i, 2)
  elif s[i] == '#': inc(i)
  let last = if maxLen == 0: s.len else: i+maxLen
  while i < last:
    case s[i]
    of '_': discard
    of '0'..'9':
      number = number shl 4 or (ord(s[i]) - ord('0'))
      foundDigit = true
    of 'a'..'f':
      number = number shl 4 or (ord(s[i]) - ord('a') + 10)
      foundDigit = true
    of 'A'..'F':
      number = number shl 4 or (ord(s[i]) - ord('A') + 10)
      foundDigit = true
    else: break
    inc(i)
  if foundDigit: result = i-start

template skipPastSep*(s: untyped, i: untyped, hitEnd: untyped, sep: char) =
  while i < s.len and s[i] != sep:
    i += 1
  if i == s.len:
    hitEnd = true
  else:
    i += 1

template skipOverWhitespace*(s: untyped, i: untyped) =
  while  i < s.len and (s[i] == ' ' or s[i] == '\t'):
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
      of Int8Col: bindSym"int8"
      of Int16Col: bindSym"int16"
      of Int32Col: bindSym"int32"
      of UIntCol: bindSym"uint64"
      of UInt8Col: bindSym"uint8"
      of UInt16Col: bindSym"uint16"
      of UInt32Col: bindSym"uint32"
      of FloatCol: bindSym"float"
      of DateCol: bindSym"Time"
    result.add(
      newIdentDefs(name = newIdentNode(col.name), kind = typ)
    )

macro schemaSeqType*(schemaType: typed): untyped =
  ## Creates a type corresponding to a given schema, containing a tuple of
  ## sequences corresponding to the `schema`. Used to cache HDF5DataFrame
  let tup = schemaType.getTypeImpl[1].getTypeImpl
  result = newNimNode(nnkTupleTy)
  for col in tup:
    var colC = copy(col)
    let name = $colC[0]
    let innerType = $colC[1]
    let dtype = nnkBracketExpr.newTree(
      ident"seq",
      ident(innerType)
    )
    colC[0] = ident(name)
    colC[1] = dtype
    result.add quote do:
      `colC`
  when defined(checkMacros):
    echo result.repr

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
      of Int8Col: bindSym"int8"
      of Int16Col: bindSym"int16"
      of Int32Col: bindSym"int32"
      of UIntCol: bindSym"uint64"
      of UInt8Col: bindSym"uint8"
      of UInt16Col: bindSym"uint16"
      of UInt32Col: bindSym"uint32"
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

  template fragmentReadStr(field: untyped, sep: char, stripQuotes: bool) =
    ## read string
    copyFrom = i
    skipPastSep(s, i, hitEnd, sep)
    if not hitEnd:
      field = substr(s, copyFrom, i-2)
    else:
      field = substr(s, copyFrom, s.len)
    if bool(stripQuotes):
      field = strip(field, chars = {'\'', '\"'})

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

      when (NimMajor, NimMinor, NimPatch) > (0, 18, 0):
        field = times.initTime(0, 0)
      else:
        field = times.Time(0)
      echo "[WARNING] Failed to parse '" & s & "' as a time (" & e.msg & "). Setting value to " & times.`$`(field)

  template createDecIntTmpl(name, fn: untyped): untyped =
    ## template to create the `fragmentRead...` templates for decimal
    ## integers.
    ## If a parsed value is larger fits into the type, an exception will
    ## be raised at runtime (from the failed conversion)
    template `name`(field: untyped): untyped =
      type dtype = type(field)
      when dtype is int64 or dtype is uint64:
        # int64 / uint64
        i += fn(s, field, start=i)
      else:
        # int8 .. int32, uint8 .. uint32
        when dtype is SomeSignedInt:
          var x: int64 = 0
        else:
          var x: uint64 = 0
        i += fn(s, x, start=i)
        field = dtype(x)

  template createNonDecIntTmpl(name, fn: untyped): untyped =
    ## template to create `fragmentRead...` templates for non decimal base.
    ## Note on non decimal uint64: values larger than uint32 cannot be
    ## parsed, since we have to use `int64` internally!
    template `name`(field: untyped): untyped =
      type dtype = type(field)
      var x: int64 = 0
      i += fn(s, x, start=i)
      field = dtype(x)

  # read binary int
  createNonDecIntTmpl(fragmentReadIntBin, parseBin)
  # read octal int
  createNonDecIntTmpl(fragmentReadIntOct, parseOct)
  # read decimal int
  createDecIntTmpl(fragmentReadIntDec, parseBiggestInt)
  # read hex int
  createNonDecIntTmpl(fragmentReadIntHex, parseHex)

  # read binary uint
  createNonDecIntTmpl(fragmentReadUIntBin, parseBin)
  # read octal uint
  createNonDecIntTmpl(fragmentReadUIntOct, parseOct)
  # read decimal uint
  createDecIntTmpl(fragmentReadUIntDec, parseBiggestUInt)
  # read hex uint
  createNonDecIntTmpl(fragmentReadUIntHex, parseHex)

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
      let call = getAst(fragmentReadStr(fieldExpr, sepExpr, col.stripQuotes))
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
    of IntCol .. Int32Col:
      case col.base
      of baseBin:
        let call = getAst(fragmentReadIntBin(fieldExpr))
        body.add(call)
      of baseOct:
        let call = getAst(fragmentReadIntOct(fieldExpr))
        body.add(call)
      of baseDec:
        let call = getAst(fragmentReadIntDec(fieldExpr))
        body.add(call)
      of baseHex:
        let call = getAst(fragmentReadIntHex(fieldExpr))
        body.add(call)
      requiresAdvancePastSep = true
    of UIntCol .. UInt32Col:
      case col.base
      of baseBin:
        let call = getAst(fragmentReadUIntBin(fieldExpr))
        body.add(call)
      of baseOct:
        let call = getAst(fragmentReadUIntOct(fieldExpr))
        body.add(call)
      of baseDec:
        let call = getAst(fragmentReadUIntDec(fieldExpr))
        body.add(call)
      of baseHex:
        let call = getAst(fragmentReadUIntHex(fieldExpr))
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
