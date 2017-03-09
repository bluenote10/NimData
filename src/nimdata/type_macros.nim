import macros

macro typedescToType(tdesc: typed): untyped =
  # echo tdesc.repr               = T
  # echo tdesc.getType.repr       = typeDesc[tuple[string, string, string, ...]]
  # echo tdesc.getTypeImpl.repr   = typeDesc[tuple[index: string, homeTeam: string, ...]]
  # getTypeImpl(df.T) itself gives a typedesc[tuple[...]] so we need
  # can simply extract the type from the child at index 1.
  result = tdesc.getTypeImpl[1]

template extractType*[T](df: DataFrame[T]): untyped =
  typedescToType(df.T)

template inspectFields*[T](df: DataFrame[T]): untyped =
  var x: typedescToType(df.T)
  x
