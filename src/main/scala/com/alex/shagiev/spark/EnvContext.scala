package com.alex.shagiev.spark

import java.net.URLClassLoader

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._
import scala.xml.XML

trait EnvContext extends Logging{
  val configRoot = "conf"
  lazy private[spark] val conf = ConfigFactory.load
  lazy private[spark] val spark = getSparkSession
  //These methods convert from Java lists/maps to Scala ones, so its easier to use
  private def toMap(hashMap: AnyRef): Map[String, AnyRef] = hashMap.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap
  def toList(list: AnyRef): List[AnyRef] = list.asInstanceOf[java.util.List[AnyRef]].asScala.toList

  val run_types_2_sizes = toMap(conf.getAnyRef("scenarios.run"))
  val l0Dir = conf.getString(s"${configRoot}.hdfs.l0-dir")
  val l1Dir = conf.getString(s"${configRoot}.hdfs.l1-dir")
  val hdfsBaseDir = conf.getString(s"${configRoot}.hdfs.base-dir")

  /**
   * Return some information on the environment we are running in.
   */
  private[spark] def versionInfo: Seq[String] = {
    val sc = getSparkSession.sparkContext
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")

    val versionInfo =
      s"""
         |---------------------------------------------------------------------------------
         | Scala version: $scalaVersion
         | Spark version: ${sc.version}
         | Spark master : ${sc.master}
         | Spark running locally? ${sc.isLocal}
         | Default parallelism: ${sc.defaultParallelism}
         |---------------------------------------------------------------------------------
         |""".stripMargin

    versionInfo.split("\n")
  }

  /**
   * Return spark session object
   *
   * NOTE Add .master("local") to enable debug via an IDE or add as a VM option at runtime
   * -Dspark.master="local[*]"
   */
  private def getSparkSession: SparkSession = {

    def get_D_Master():Option[String]={
      sys.env.get("master")
    }

    val sparkSession = SparkSession.builder
      .appName(conf.getString(s"${configRoot}.spark.appname"))

    var sparkConf = new SparkConf()

    if (!get_D_Master().isEmpty){
      sparkConf.setMaster(get_D_Master().get)
    }
    sparkSession.config(sparkConf)


   sparkSession.getOrCreate()
  }


  /*
  * Dump spark configuration for the current spark session.
  */
  private[spark] def getAllSparkConf: String = {
    getSparkSession.conf.getAll.map {
      case (k, v) => "Key: [%s] Value: [%s]" format(k, v)
    } mkString("", "\n", "\n")
  }


}

