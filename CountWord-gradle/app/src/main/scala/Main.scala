import csv.{CSVWriter, CliParamsSerializer}

import scala.annotation.tailrec
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object Main {

  var filename: String = ""
  var storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  var partition: Int = 4
  var replicate: Int = 1
  var csvWriter: csv.CSVWriter = _

  @tailrec
  def parseArgs(args: List[String]): Unit = {
    args match {
      case "-filename" :: arg :: rest => {
        filename = arg
        parseArgs(rest)
      }
      case "-storageLevel" :: arg :: rest => {
        storageLevel = StorageLevel.fromString(arg)
        parseArgs(rest)
      }
      case "-partition" :: arg :: rest => {
        partition = arg.toInt
        parseArgs(rest)
      }
      case "-replicate" :: arg :: rest => {
        replicate = arg.toInt
        parseArgs(rest)
      }
      case "-metricsCsv" :: arg :: rest => {
        csvWriter = new CSVWriter(arg)
        parseArgs(rest)
      }
      case _ :: rest => {
        parseArgs(rest)
      }
      case List() =>
    }
  }

  def context: SparkContext = {
    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[4]")
    conf.setIfMissing("spark.app.name", "SparkTE")
    SparkContext.getOrCreate(conf)
  }

  val mb: Int = 1024 * 1024
  val runtime: Runtime = Runtime.getRuntime

  def nFile(filename: String, n: Int, spark: SparkContext, p: Int): RDD[String] = {
    var res = spark.textFile(filename, p)
    val ite = Math.min(1, n / p)
    for (_ <- 1 until ite) {
      res = res.union(spark.textFile(filename, p))
    }
    res
  }

  def main(args: Array[String]): Unit = {
    // initialize spark
    val spark = context
    parseArgs(args.toList)
    val cliParams = CliParamsSerializer.serialize(args)

    // initialize CSV writer
    csvWriter.createCsv()

    for (i <- 1 to 3) {
      val t0 = System.nanoTime()
      val m0 = (runtime.totalMemory - runtime.freeMemory) / mb
      val file: RDD[String] = nFile(filename, replicate, spark, partition)

      // do the count word
      val res = file.flatMap(line => line.split(" ")).map(word => (word.replaceAll("[-+.^:,;)(_]", ""), 1)).reduceByKey(_ + _).sortBy(e => e._2, ascending = false).collect()
      val t1 = System.nanoTime()
      val m1 = (runtime.totalMemory - runtime.freeMemory) / mb

      val computationTime: Long = Math.round((t1 - t0) / 1e6)
      println("Computation time = %d ms".format(computationTime))

      val usedMemory: Long = m1 - m0
      println("Used memory = %d mb".format(usedMemory))

      println(res(0))

      // save metrics
      csvWriter.writeToCsv(cliParams, "[cpu(ms),memory(MB)]", "[%d,%d]".format(computationTime, usedMemory))
    }
  }

}
