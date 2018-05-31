import macros
import strutils
from nimdata import useNimDevel

# Inspired by Jehan's forum post
proc `|`*(s: string, d: int): string =
  if s.len < d.abs:
    let pad = spaces(d.abs - s.len)
    if d >= 0:
      result = pad & s
    else:
      result = s & pad
  else:
    result = s

proc `|`*(x: int, d: int): string =
  result = $x
  let pad = spaces(d.abs - result.len)
  if d >= 0:
    result = pad & result
  else:
    result = result & pad

proc `|`*(f: float, d: tuple[w, p: int]): string =
  result = formatFloat(f, ffDecimal, d.p)
  let pad = spaces(d.w.abs - result.len)
  if d.w >= 0:
    result = pad & result
  else:
    result = result & pad

proc `|`*(f: float, d: int): string =
  $f | d

proc fixedTruncateR*(s: string, maxLen: int): string =
  if s.len > maxLen:
    result = s.substr(0, maxLen - 2) & "…"
  else:
    result = s

proc fixedTruncateL*(s: string, maxLen: int): string =
  if s.len > maxLen:
    result = "…" & s.substr(s.len - maxLen + 1, s.len)
  else:
    result = s

macro debug*(n: varargs[typed]): untyped =
  # `n` is a Nim AST that contains the whole macro invocation
  # this macro returns a list of statements:
  result = newNimNode(nnkStmtList, n)
  # iterate over any argument that is passed to this macro:
  for i in 0..n.len-1:
    # add a call to the statement list that writes the expression;
    # `toStrLit` converts an AST to its string representation:
    add(result, newCall("write", newIdentNode("stdout"), toStrLit(n[i])))
    # add a call to the statement list that writes ": "
    add(result, newCall("write", newIdentNode("stdout"), newStrLitNode(": ")))
    # add a call to the statement list that writes the expressions value:
    #add(result, newCall("writeln", newIdentNode("stdout"), n[i]))
    add(result, newCall("write", newIdentNode("stdout"), n[i]))
    # separate by ", "
    if i != n.len-1:
      add(result, newCall("write", newIdentNode("stdout"), newStrLitNode(", ")))

  # add new line
  add(result, newCall("writeLine", newIdentNode("stdout"), newStrLitNode("")))

when useNimDevel:
  macro showExpr*(arg: varargs[untyped]): untyped =
    let argCallsite = arg[1]
else:
  macro showExpr*(arg: typed): untyped =
    let argCallsite = callsite()[1]
    result = newNimNode(nnkStmtList)
    result.add(newCall("writeLine", newIdentNode("stdout"), argCallsite.toStrLit))
    #result.add(newCall("write", newIdentNode("stdout"), newStrLitNode(" => ")))
    result.add(newCall("writeLine", newIdentNode("stdout"), arg))

when useNimDevel:
  macro showStmt*(arg: varargs[untyped]): untyped =
    let argCallsite = arg[1]
else:
  macro showStmt*(arg: typed): untyped =
    let argCallsite = arg[1]
    result = newNimNode(nnkStmtList)
    result.add(newCall("writeLine", newIdentNode("stdout"), argCallsite.toStrLit))
    #result.add(newCall("write", newIdentNode("stdout"), newStrLitNode(" => ")))
    result.add(arg)

template scope*(name: string, code: untyped): untyped =
  echo "\n *** ", name
  block:
    code


proc seqAddr*[T](s: var seq[T]): ptr T =
  if s.len > 0:
    result = s[0].addr
  else:
    result = nil

proc getFields*(T: typedesc): seq[string] =
  var x: T
  result = @[]
  for field, _ in x.fieldPairs:
    result.add(field)


template UnitTestSuite*(name: string, code: untyped): untyped =
  when defined(testNimData):
    import unittest
    suite(name):
      code

template notCompiles*(e: untyped): untyped =
  not compiles(e)

template getSourcePath*(): string =
  let path = instantiationInfo(fullPaths=true)
  path.filename

