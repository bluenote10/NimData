import zip/zlib
import streams

#[
Code inspired by zah's answer: http://stackoverflow.com/a/33104286/1804173
]#

type
  GZipStream* = object of StreamObj
    f: GzFile

  GZipStreamRef* = ref GZipStream


proc fsClose(s: Stream) =
  discard gzclose(GZipStreamRef(s).f)

proc fsReadData(s: Stream, buffer: pointer, bufLen: int): int =
  return gzread(GZipStreamRef(s).f, buffer, bufLen)

proc fsAtEnd(s: Stream): bool =
  return gzeof(GZipStreamRef(s).f) != 0

proc newGZipStream*(f: GzFile): GZipStreamRef =
  new result
  result.f = f
  result.closeImpl = fsClose
  result.readDataImpl = fsReadData
  result.atEndImpl = fsAtEnd
  # other methods are nil!

proc newGZipStream*(filename: string): GZipStreamRef =
  var gz = gzopen(filename, "rb")
  if gz != nil:
    return newGZipStream(gz)
  else:
    raise newException(IOError, "Can't open '" & filename & "'")