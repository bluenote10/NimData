import macros

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

macro showExpr*(arg: untyped): untyped =
  let argCallsite = callsite()[1]
  result = newNimNode(nnkStmtList)
  result.add(newCall("writeLine", newIdentNode("stdout"), argCallsite.toStrLit))
  #result.add(newCall("write", newIdentNode("stdout"), newStrLitNode(" => ")))
  result.add(newCall("writeLine", newIdentNode("stdout"), arg))

macro showStmt*(arg: untyped): untyped =
  let argCallsite = callsite()[1]
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

template UnitTestSuite*(name: string, code: untyped): untyped =
  when defined(testNimData):
    import unittest
    suite(name):
      code

template getSourcePath*(): string =
  let path = instantiationInfo(fullPaths=true)
  path.filename

