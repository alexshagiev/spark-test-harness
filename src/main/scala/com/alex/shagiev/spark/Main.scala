package com.alex.shagiev.spark

import com.alex.shagiev.jsonl.JsonlDfParser
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
        val dInit = timer {
          val linesRDD = sc.textFile(l0FullPath)
          val headerLine = linesRDD.first()
          val metaData = JsonlDfParser.parseDfMetaData(headerLine)
          val rowsRDD = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaData))
          jsonlDF = spark.createDataFrame(rowsRDD, JsonlDfParser.meta2SparkSchema(metaData))
        }
        logger.info(s"Init,\t ${dInit.toMinutes}min,\t ${dInit.toSeconds}sec,\t ${dInit.toMillis}ms,\t spark.cores.max=${sparkCoresMax},\t ${l0FullPath}")
        val dCount = timer{jsonlDF.count()}
        logger.info(s"Count,\t ${dCount.toMinutes}min,\t ${dCount.toSeconds}sec,\t ${dCount.toMillis}ms,\t spark.cores.max=${sparkCoresMax},\t ${l0FullPath}")
        val dSave = timer{jsonlDF.write.mode(SaveMode.Overwrite).save(l1FullPath)}
        logger.info(s"Save,\t ${dSave.toMinutes}min,\t ${dSave.toSeconds}sec,\t ${dSave.toMillis}ms,\t spark.cores.max=${sparkCoresMax},\t ${l1FullPath}"
        )
      }

    }
  }

}