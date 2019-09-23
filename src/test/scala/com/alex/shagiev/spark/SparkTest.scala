package com.alex.shagiev.spark

import java.io.InputStream

import grizzled.slf4j.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{Outcome, fixture}
import com.alex.shagiev.jsonl.{JsonlDfMetaData, JsonlDfParser}
import com.typesafe.config.ConfigFactory

class SparkTest extends fixture.FunSuite with Logging {

  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val conf = ConfigFactory.load
    val sparkSession = SparkSession.builder
      .appName(conf.getString("performance_harness.app_name"))
      .master(conf.getString("performance_harness.master"))
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }


  test("parse JSONL DataFrame@basic") { spark =>
    val linesRDD = spark.sparkContext.parallelize(getInputData("/data/large_file_random_col_10_row_10_date_2019-09-02.jsonl"), 4)
    val headerLine = linesRDD.first()
    assert(linesRDD.count() === 11)
    assert(linesRDD.filter(!_.contains(headerLine)).count() === 10)

    val metaData = JsonlDfParser.parseDfMetaData(headerLine)
    logger.info("Extracted & parsed metadata successfully:")
    logger.info(metaData)
    val rows = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaData)).collect()
    logger.info("Rows Extracted & Parsed successfully:")
    rows.foreach(r => logger.info(JsonlDfParser.toString(r)))

  }
  test( "parse JSONL DataFrame@partial schema"){spark=>
    val linesRDD = spark.sparkContext.parallelize(getInputData("/data/large_file_random_col_10_row_10_date_2019-09-02.jsonl"), 4)
    val headerLine = linesRDD.first()
    val metaDataSource = JsonlDfParser.parseDfMetaData(headerLine)
    logger.info("Extracted & parsed metadata successfully:")
    logger.info(metaDataSource)
    //    create partial schema
    val metaDataTargetException = new JsonlDfMetaData( Array("biz_date","a_xxxxx","b_cvdxurcjmxfmmaj"), Array("Date","String","Timestamp"),1)
    logger.info("Created target metadata that should fail:")
    logger.info(metaDataTargetException)

    assertThrows[Exception] {
      val rows = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaDataSource, metaDataTargetException)).collect()
    }
    val metaDataTargetSuccess = new JsonlDfMetaData( Array("biz_date","a_fejcyllnrtyexcn","b_cvdxurcjmxfmmaj"), Array("Date","String","Timestamp"),3)

    logger.info("Created target metadata that should succeed:")
    logger.info(metaDataTargetSuccess)
    val rows = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaDataSource, metaDataTargetSuccess)).collect()
    assert( rows(0).length === 3)
    rows.foreach(r => logger.info(JsonlDfParser.toString(r)))
  }

  test("parse JSONL DataFrame@RDD 2 spark.DataFrame conversion") { spark =>
    val linesRDD = spark.sparkContext.parallelize(getInputData("/data/large_file_random_col_20_row_10_date_2019-09-10.jsonl"), 4)
    val headerLine = linesRDD.first()
    val metaData = JsonlDfParser.parseDfMetaData(headerLine)
    logger.info("Extracted & parsed metadata successfully:")
    logger.info(metaData)


    val rowsRDD = linesRDD.filter(!_.contains(headerLine)).map(JsonlDfParser.parseDfRow(_, metaData))


    val rowsDF = spark.createDataFrame(rowsRDD, JsonlDfParser.meta2SparkSchema(metaData))
    logger.info("DataFrame Created from & Parsed successfully:")
    logger.info(rowsDF)
    rowsDF.createOrReplaceTempView("temp")
    val result = spark.sql("select * from temp")
    rowsDF.show()

    rowsDF.write.mode(SaveMode.Overwrite).save("./target/test-parquet/example.parquet")

  }

  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
}
