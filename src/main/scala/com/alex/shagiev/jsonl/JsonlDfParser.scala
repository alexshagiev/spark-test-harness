package com.alex.shagiev.jsonl

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, types}
import org.json4s.jackson.JsonMethods.parse
import java.time.LocalDate
import java.time.LocalDateTime

import org.apache.spark.sql.types._
import org.json4s.DefaultFormats

import scala.collection.mutable.ArrayBuffer
import scala.runtime.RichBoolean




case class JsonlDfMetaData(val columnNames: Array[String], val types: Array[String], val columnNumber: Int) extends Serializable {
  override def toString: String = {
    return "types: " + types.mkString("[", ",", "]") +
      ", columnNames: " + columnNames.mkString("[", ",", "]") +
      ", columnNumber: " + columnNumber
  }
}
  //object JsonlDfMetaData{
  //  def apply(columnNames: Array[String], types: Array[String], columnNumber: Int): JsonlDfMetaData = {
  //    if ( columnNames.length != types.length || columnNames.length != columnNumber) throw new Exception("Invalid Argument")
  //    new JsonlDfMetaData(columnNames, types, columnNumber)
  //  }
  //}

object JsonlDfParser {


  private val t_Date_format = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")
  private val t_Timestamp_format = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.n")



  def parseDfMetaData(jsonldf_header: String): JsonlDfMetaData = {
    implicit val formats = DefaultFormats
    val columnNames = (parse(jsonldf_header) \ "metaData" \ "columnNames").extract[Array[String]]
    val columnTypes = (parse(jsonldf_header) \ "metaData" \ "types").extract[Array[String]]
    if (columnNames.length != columnTypes.length){throw new Exception("Invalid JsonL DataFrame metadata, number of columns does not match number of provided datatypes")}
    JsonlDfMetaData(columnNames, columnTypes, columnNames.length)
  }

  def meta2SparkSchema(jsonlDfMetaData: JsonlDfMetaData): StructType = {
    var fields = Array[StructField]()
    for (i <- 0 until jsonlDfMetaData.columnNames.length){
      val name = jsonlDfMetaData.columnNames(i)
      val typ = jsonlDfMetaData.types(i)
      var df_type = typ match {
        case "String" => StringType
        case "Date" => DateType
        case "Timestamp" => TimestampType
        case "BigDecimal" => DecimalType.SYSTEM_DEFAULT
        case "BigInteger" => LongType
        case "Boolean" => BooleanType
        case "Array<BigDecimal>" => ArrayType(DecimalType.SYSTEM_DEFAULT)
        case "Array<BigInteger>" => ArrayType(LongType)
        case _ => StringType
       }
      val field = StructField(name, df_type,true)
      fields  = fields :+ field
    }
//    val fields = jsonlDfMetaData.columnNames.map(name => name match {
//      case "String" => StructField (name, StringType, nullable = true)
//    }
//    )
    StructType(fields)
  }

//  def targetMeta2sourceIndex(jsonldf_source_metadata: JsonlDfMetaData, jsonldf_target_metadata: JsonlDfMetaData): IndexedSeq[Int] = {
  def targetMeta2sourceIndex(jsonldf_source_metadata: JsonlDfMetaData, jsonldf_target_metadata: JsonlDfMetaData): Array[Int] = {
    if (jsonldf_source_metadata.equals(jsonldf_target_metadata) != true) {

      var result = Array[Int]()
      for (index_target <- 0 until jsonldf_target_metadata.columnNames.length) {
        val name_target = jsonldf_target_metadata.columnNames(index_target)
        val index_source = jsonldf_source_metadata.columnNames.indexOf(name_target)
        if (index_source == -1) {
          throw new Exception("Column '" + jsonldf_target_metadata.columnNames(index_target) + "' , specified in the target schema, does not exist in the source schema")
        }
        val type_source = jsonldf_source_metadata.types(index_source)
        val type_target = jsonldf_target_metadata.types(index_target)

          if (!type_source.equals(type_target)) {
            val name_source = jsonldf_source_metadata.types(index_source)
            throw new Exception("Column '" + name_target + "' Datatype mismatch. type '" + type_target + "' target and type '" + type_source + "' source")
          } else {
            result = result :+ index_source
          }

      }
      result
    } else {
       0 until jsonldf_source_metadata.columnNames.length toArray
//      for (i <- 0 until jsonldf_source_metadata.columnNames.length) yield i
    }
  }

  def toString(row: Row, include_type: Boolean = true): String = {
    def shortenPkg(cls: Class[_]): String = {
      var strPkg = (if (cls == null) "Nil" else cls.toString.replace("class ", ""))
      var arrPkg = strPkg.split('.')
      var buf = new StringBuilder
      for (i <- 0 until arrPkg.length) {
        buf ++= (if (i == arrPkg.length - 1) (arrPkg(i)) else arrPkg(i).charAt(0) + ".")
      }
      buf.toString
    }

    def typeWrap(v: Any): String = {
      if (include_type == true) {
        (if (v == null) "Nil" else shortenPkg(v.getClass)) + "(" + v + ")"
      } else {
        v.toString
      }
    }

    def typeWrapA(a: Array[Any]): String = {
      var buf = new StringBuilder
      if (include_type == true) {
        buf ++= "s.Array<" + shortenPkg(a(0).getClass) + ">"
      }
      buf ++= "["
      for (i <- 0 until a.length) {
        if (i != 0) {
          buf ++= ","
        }
        buf ++= a(i) + ""
      }
      buf ++= "]"
      buf.toString
    }


    val buf = new StringBuilder
    buf ++= "["
    for (i <- 0 until row.length) {
      if (i != 0) {
        buf ++= ","
      }
//      buf ++= Representable.represent(row.get(i))
      if (row.get(i).isInstanceOf[Array[Any]]) {
        buf ++= typeWrapA(row.get(i).asInstanceOf[Array[Any]])
      } else {
        buf ++= typeWrap(row.get(i))
      }
    }
    buf ++= "]"
    buf.toString
  }

//  def parseDfRow(jsonldf_row: String, jsonldf_source_metadata: JsonlDfMetaData, jsonldf_target_metadata: JsonlDfMetaData = null, jsonldf_target_2_source_index: IndexedSeq[Int] = null): Row = {
  def parseDfRow(jsonldf_row: String, jsonldf_source_metadata: JsonlDfMetaData, jsonldf_target_metadata: JsonlDfMetaData = null, jsonldf_target_2_source_index: Array[Int] = null): Row = {
//    Row(1,2)
    implicit val formats = DefaultFormats
    val values = (parse(jsonldf_row) \ "row").extract[Array[Any]]

    // if target/override fields metadata object was not provided assume entire row needs to be extracted based on the header/source metadata
    val jsonldf_target_metadata_local = if (jsonldf_target_metadata != null) jsonldf_target_metadata else jsonldf_source_metadata
    val jsonldf_target_2_source_index_local = if (jsonldf_target_2_source_index != null) jsonldf_target_2_source_index else targetMeta2sourceIndex(jsonldf_source_metadata, jsonldf_target_metadata_local)
//    val jsonldf_target_2_source_index_local = targetMeta2sourceIndex(jsonldf_source_metadata, jsonldf_target_metadata_local)

    var i = 0
    var row_buffer = new Array[Any](jsonldf_target_2_source_index_local.length)
    while (i < jsonldf_target_2_source_index_local.length) {
      val source_index = jsonldf_target_2_source_index_local(i)
      if (source_index == -1) {
        throw new Exception("Index of columns to extract contains invalid reference. " + jsonldf_target_2_source_index.mkString("[", ",", "]"))
      }
      val v = values(source_index)
      jsonldf_source_metadata.types(source_index) match {
        case "Date" => row_buffer(i) = dateT(v)
        case "Timestamp" => row_buffer(i) = dateTimeT(v)
        case "String" => row_buffer(i) = strT(v)
        case "BigDecimal" => row_buffer(i) = decimalT(v)
        case "BigInteger" => row_buffer(i) = intT(v)
        case "Boolean" => row_buffer(i) = boolT(v)
        case "Array<BigDecimal>" => row_buffer(i) = arrTBigD(v)
        case "Array<BigInteger>" => row_buffer(i) = arrTBigI(v)
        case _ => row_buffer(i) = if (v == null) v else v.toString
      }
      i += 1
    }
    Row.fromSeq(row_buffer)
  }

  private def dateT(s: Any): Date = {
      if (s == null) null else Date.valueOf(LocalDate.parse(s.toString, JsonlDfParser.t_Date_format))
  }

  private def dateTimeT(s: Any): Timestamp = {
    //    TODO figure out how to include .000SSS seconds.
    if (s == null) null else Timestamp.valueOf(LocalDateTime.parse(s.toString + "000", JsonlDfParser.t_Timestamp_format))
  }

  private def strT(s: Any): String = {
    if (s == null) null else s.toString
  }

  private def decimalT(s: Any): BigDecimal = {
    if (s == null) null else BigDecimal(s.toString)
  }
  private def intT(s: Any): Any = {
    if (s == null) null else s.toString.toLong
  }

  private def boolT(s: Any): Any = {
    if (s == null) null else s.toString.toBoolean//if (s.asInstanceOf[Boolean] == true) true else false
  }

  private def arrTBigD(s: Any): Array[BigDecimal] = {
    if (s == null) {
      return null
    } else {
      // TODO fix type of Int to BigDecimal
      val l = s.asInstanceOf[List[Double]]
      var arr = new Array[BigDecimal](l.length)
//      var arr = ArrayBuffer[BigDecimal](l.length)
      var i = 0
      while (i < l.length) {
        arr(i) = BigDecimal(l(i).toString)
        i += 1
      }
      return arr
    }
  }
  private def arrTBigI(s: Any): Array[Long] = {
    if (s == null) {
      return null
    } else {
      // TODO fix type of Int to BigDecimal
      val l = s.asInstanceOf[List[BigInt]]
      var arr = new Array[Long](l.length)
//      var arr = ArrayBuffer[Long](l.length)
      var i = 0
      while (i < l.length) {
        arr(i) = l(i).toString.toLong
        i += 1
      }
      return arr
    }
  }




}