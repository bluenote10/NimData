import nimhdf5
import nimdata
import typetraits

const InGridSchema = [
  intCol("eventNumber"),
  floatCol("length"),
  floatCol("width"),
  floatCol("skewnessLongitudinal"),
  floatCol("skewnessTransverse"),
  floatCol("kurtosisLongitudinal"),
  floatCol("kurtosisTransverse"),
  floatCol("rotationAngle"),
  floatCol("eccentricity"),
  floatCol("fractionInTransverseRms"),
  floatCol("lengthDivRmsTrans"),
]

const FadcSchema = [
  uint16Col("argMinval"),
  floatCol("minvals"),
  floatCol("baseline"),
  intCol("eventNumber"),
  uint16Col("fallTime"),
  uint16Col("riseTime"),
  uint16Col("riseStart"),
  uint16Col("fallStop")
]


type outType = schemaType(InGridSchema)
type outFadc = schemaType(FadcSchema)

var names: seq[string]
var tmp: outType
echo names
let df = fromHDF5[outType](DF,
                           "run_124.h5",
                           "/reconstruction/run_124/chip_3")

var it = df.iter()
for x in toIterBugfix(it):
  echo x
  break

let dfCached = df.cache()

dfCached.map(record => record.projectTo(eventNumber, length))
  .filter(record => (record.eventNumber >= 1000 and record.eventNumber <= 1200))
  .take(15)
  .show()

dfCached.map(record => record.projectTo(eventNumber, length))
  .drop(15000)
  .take(5)
  .show()

#echo df

var h5f = H5File("run_124.h5", "r")
h5f.visit_file()
let dfOpen = fromHDF5[outType](DF,
                               h5f,
                               "run_124.h5",
                               "/reconstruction/run_124/chip_3")

dfOpen.map(record => record.projectTo(eventNumber, eccentricity))
  .drop(15000)
  .filter(record => record.eccentricity <= 1.4)
  .take(5)
  .show()

let dfFadc = fromHDF5[outFadc](DF,
                               "run_124.h5",
                               "/reconstruction/run_124/fadc")
dfFadc.take(5).show()
