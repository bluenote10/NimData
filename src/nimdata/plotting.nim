import sugar
import sequtils
export map

import plotly
export plotly

template barPlot*[T](df: DataFrame[T], x, y: untyped): untyped =
  let data = df.collect()
  let xData = data.map(r => r.x)
  let yData = data.map(r => r.y)
  let title = "Bar plot of " & astToStr(x) & " vs. " & astToStr(y)
  barPlot(xData, yData)
    .title(title)
    .xlabel(astToStr(x))
    .ylabel(astToStr(y))

template histPlot*[T](df: DataFrame[T], hist: untyped): untyped =
  let data = df.collect()
  let histData = data.map(r => r.hist)
  let title = "Histogram of " & astToStr(hist)
  histPlot(histData)
    .title(title)
    .xlabel(astToStr(hist))

template heatmap*[T](df: DataFrame[T], x, y, z: untyped): untyped =
  let data = df.collect()
  let xData = data.map(r => r.x)
  let yData = data.map(r => r.y)
  let zData = data.map(r => r.z)
  let title = "Heatmap of " & astToStr(x) & " vs. " & astToStr(y) & " on " & astToStr(z)
  heatmap(xData, yData, zData)
    .title(title)
    .xlabel(astToStr(x))
    .ylabel(astToStr(y))

template scatterPlot*[T](df: DataFrame[T], x, y: untyped): untyped =
  let data = df.collect()
  let xData = data.map(r => r.x)
  let yData = data.map(r => r.y)
  let title = "Scatter plot of " & astToStr(x) & " vs. " & astToStr(y)
  scatterPlot(xData, yData)
    .title(title)
    .xlabel(astToStr(x))
    .ylabel(astToStr(y))

template scatterColor*[T](df: DataFrame[T], x, y, z: untyped): untyped =
  ## adds a color dimension to the scatter plot in addition
  let data = df.collect()
  let xData = data.map(r => r.x)
  let yData = data.map(r => r.y)
  let zData = data.map(r => r.z)
  let title = "Scatter plot of " & astToStr(x) & " vs. " & astToStr(y) &
    " with colorscale of " & astToStr(z)
  scatterColor(xData, yData, zData)
    .title(title)
    .xlabel(astToStr(x))
    .ylabel(astToStr(y))
