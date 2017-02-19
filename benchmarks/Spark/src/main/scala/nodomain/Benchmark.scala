package nodomain

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class Stopwatch {
  private var startTime = System.nanoTime() 
  
  def stop(): Double = {
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000000
  }
}


object Benchmark {

  def runTimed(name: String, numRepeats: Int = 3)(body: => Unit): String = {
    val runTimes = Array.tabulate[Double](numRepeats){ i =>
      println(f"\n *** Running: $name [Iteration: ${i+1}]")
      val t1 = System.nanoTime()
      body
      val t2 = System.nanoTime()
      (t2 - t1).toDouble / 1000000000
    }
    val min = runTimes.reduce(_ min _)
    val mean = runTimes.reduce(_ + _) / runTimes.length
    val max = runTimes.reduce(_ max _)
    val resultString = f"$name%-40s    min: $min%6.3f    mean: $mean%6.3f    max: $max%6.3f"
    resultString
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Error: Expected one argument (for input file).")
      System.exit(1)
    }
    val file = args(0)
    runBenchmark(file)
  }
  
  def runBenchmark(file: String) {
    implicit val spark = SparkSession
      .builder()
      .appName("Benchmark")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    implicit val sc = spark.sparkContext
  
    def loadCsv(): DataFrame = {
      val df = spark.read
                    .format("csv")
                    .option("delimiter", ",")
                    .option("header", "false")
                    .option("inferSchema", "true")
                    .load(file)
      // df.printSchema()
      df
    }
    
    val resultStrings = collection.mutable.ArrayBuffer[String]()
    
    resultStrings += runTimed("Count"){
      val df = loadCsv()
      println("Number of lines: " + df.count())
    }
    
    resultStrings += runTimed("Column averages"){
      val df = loadCsv().cache()
      println("Average col 1: " + df.select(avg($"_c0")).head())
      println("Average col 2: " + df.select(avg($"_c1")).head())
      println("Average col 3: " + df.select(avg($"_c2")).head())
      println("Average col 4: " + df.select(avg($"_c3")).head())
    }
    
    sc.stop()
    println("\n *** Summary:")
    println(resultStrings.mkString("\n"))
  }
  
}

