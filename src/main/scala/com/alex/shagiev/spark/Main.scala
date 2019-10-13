package com.alex.shagiev.spark

import java.sql.Date
import java.util.UUID

import com.alex.shagiev.jsonl.JsonlDfParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.jboss.netty.handler.codec.spdy.DefaultSpdyDataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


case class TestHarnessReport(runId: String, date: Date, timedUnit: String, run_type: String, run_size: String, min: Long, sec: Long, mil: Long, sparkCoreMax: String, master: String, path: String)

object Main extends EnvContext {
  def timer[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    Duration(t1 - t0, NANOSECONDS)
  }

  def main(args: Array[String]): Unit = {


    val sc = spark.sparkContext
    sc.getConf.getAll.foreach(s => logger.info(s"Spark Configuration - ${s}"))
    val sparkCoresMax = sc.getConf.get("spark.cores.max", "NOT_SET")
    val master = sc.getConf.get("spark.master")

    var performanceIndicators = ListBuffer[TestHarnessReport]()
    val randomUUID = UUID.randomUUID().toString
    val currentDate = new Date(new java.util.Date().getTime)
    var resultDF: DataFrame = null
    val baseDir = conf.getString(s"conf.hdfs.base-dir")

    for ((run_type, run_sizes) <- this.run_types_2_sizes) {

      for (run_size <- this.toList(run_sizes)) {
        var inputFile = conf.getString(s"scenarios.${run_type}.${run_size}.name")
        var inputDir = "hdfs://" + conf.getString(s"scenarios.${run_type}.l0-dir")
        var outputDir = "hdfs://" +  conf.getString(s"scenarios.${run_type}.l1-dir")
        val l0FullPath = s"${inputDir}/${inputFile}"
        val l1FullPath = s"${outputDir}/${inputFile}"

        var jsonlDF: org.apache.spark.sql.DataFrame = null
        var linesRDD: RDD[String] = null
        var timed = timer {
          logger.info(s"Processing file: ${l0FullPath}")
          linesRDD = sc.textFile(l0FullPath)
          val headerLine = linesRDD.first()
          val metaData = JsonlDfParser.parseDfMetaData(headerLine)
          val rowsRDD = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaData))
          jsonlDF = spark.createDataFrame(rowsRDD, JsonlDfParser.meta2SparkSchema(metaData))
        }
        var reportEntry = TestHarnessReport(randomUUID, currentDate, "Init", run_type, run_size.toString, timed.toMinutes, timed.toSeconds, timed.toMillis, sparkCoresMax, master, l0FullPath)
        logger.info(reportEntry)
        performanceIndicators += reportEntry

        timed = timer {
          linesRDD.count()
        }
        reportEntry = TestHarnessReport(randomUUID, currentDate, "Raw Text Lines/Count", run_type, run_size.toString, timed.toMinutes, timed.toSeconds, timed.toMillis, sparkCoresMax, master, l0FullPath)
        logger.info(reportEntry)
        performanceIndicators += reportEntry

        timed = timer {
          jsonlDF.count()
        }
        reportEntry = TestHarnessReport(randomUUID, currentDate, "Parsed DF Rows", run_type, run_size.toString, timed.toMinutes, timed.toSeconds, timed.toMillis, sparkCoresMax, master, l0FullPath)
        logger.info(reportEntry)
        performanceIndicators += reportEntry

        timed = timer {
          jsonlDF.write.mode(SaveMode.Overwrite).save(l1FullPath)
        }
        reportEntry = TestHarnessReport(randomUUID, currentDate, "Save DF Parquet", run_type, run_size.toString, timed.toMinutes, timed.toSeconds, timed.toMillis, sparkCoresMax, master, l1FullPath)
        logger.info(reportEntry)
        performanceIndicators += reportEntry


        timed = timer {
          val parqDF = spark.read.parquet(l1FullPath); parqDF.count()
        }
        reportEntry = TestHarnessReport(randomUUID, currentDate, "Read Parquet DF/Count", run_type, run_size.toString, timed.toMinutes, timed.toSeconds, timed.toMillis, sparkCoresMax, master, l1FullPath)
        logger.info(reportEntry)
        performanceIndicators += reportEntry

        import spark.implicits._
        resultDF = performanceIndicators.toDF()
        resultDF.show(10 * 100)
      }

    }
    resultDF.coalesce(1).write.format("csv").option("header", "true").mode(SaveMode.Append).save(s"hdfs://${baseDir}/performance-stats.csv");
  }

}