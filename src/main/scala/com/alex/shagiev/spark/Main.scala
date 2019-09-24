package com.alex.shagiev.spark

import com.alex.shagiev.jsonl.JsonlDfParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.jboss.netty.handler.codec.spdy.DefaultSpdyDataFrame

import scala.concurrent.duration._

object Main extends EnvContext {
  def timer[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    Duration(t1 - t0,NANOSECONDS)
//    return (result, elapsedMs)
//    elapsedMs
  }

  def main(args: Array[String]): Unit = {


    val sc = this.spark.sparkContext
    sc.getConf.getAll.foreach(s=>logger.info(s"Spark Configuration - ${s}"))
    val sparkCoresMax = sc.getConf.get("spark.cores.max","MAX")

    for ( (run_type,run_sizes) <- this.run_types_2_sizes) {

      for( run_size <- this.toList(run_sizes)){
        var inputFile = this.conf.getString(s"scenarios.${run_type}.${run_size}.name")
        var inputDir = this.conf.getString(s"scenarios.${run_type}.l0-dir")
        var outputDir = this.conf.getString(s"scenarios.${run_type}.l1-dir")
        val l0FullPath = s"${this.hdfsUrl}/${inputDir}/${inputFile}"
        val l1FullPath = s"${this.hdfsUrl}/${outputDir}/${inputFile}"

        var jsonlDF: org.apache.spark.sql.DataFrame = null
        var linesRDD: RDD[String] = null
        val dInit = timer {
          linesRDD = sc.textFile(l0FullPath)
          val headerLine = linesRDD.first()
          val metaData = JsonlDfParser.parseDfMetaData(headerLine)
          val rowsRDD = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaData))
          jsonlDF = spark.createDataFrame(rowsRDD, JsonlDfParser.meta2SparkSchema(metaData))
        }
        logger.info(s"Init,(scenario.min.sec.ms.cores.file),${run_type}.${run_size},${dInit.toMinutes},${dInit.toSeconds},${dInit.toMillis},${sparkCoresMax},${l0FullPath}")
        val dLinesCount = timer{linesRDD.count()}
        logger.info(s"Count-Lines,(scenario.min.sec.ms.cores.file),${run_type}.${run_size},${dLinesCount.toMinutes},${dLinesCount.toSeconds},${dLinesCount.toMillis},${sparkCoresMax},${l0FullPath}")

        val dParsedCount = timer{jsonlDF.count()}
        logger.info(s"Count-Parsed,(scenario.min.sec.ms.cores.file),${run_type}.${run_size},${dParsedCount.toMinutes},${dParsedCount.toSeconds},${dParsedCount.toMillis},${sparkCoresMax},${l0FullPath}")

        val dSave = timer{jsonlDF.write.mode(SaveMode.Overwrite).save(l1FullPath)}
        logger.info(s"Save-Parsed,(scenario.min.sec.ms.cores.file),${run_type}.${run_size},${dSave.toMinutes},${dSave.toSeconds},${dSave.toMillis},${sparkCoresMax},${l1FullPath}"
        )

        val dParqueCount = timer{val parqDF = spark.read.parquet(l1FullPath); parqDF.count()}
        logger.info(s"Count-Parquet,(scenario.min.sec.ms.cores.file),${run_type}.${run_size},${dParqueCount.toMinutes},${dParqueCount.toSeconds},${dParqueCount.toMillis},${sparkCoresMax},${l1FullPath}")

      }

    }
  }

}