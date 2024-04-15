package sofian.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import sofian.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  override def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession.read.options(options).format(format).load(path)
  }

  private def detectSeparator(path: String): String = {
    val sample = sparkSession.read.textFile(path).take(100).mkString("\n")
    val separators = Seq(',', ';', '\t', '|', ' ')
    val separatorCounts = separators.map(sep => sep -> sample.count(_ == sep)).toMap
    separatorCounts.maxBy(_._2)._1.toString
  }

  def readCsv(path: String, separator: Option[String] = None, schema: Option[StructType] = None, header: Boolean = true): DataFrame = {
    val sep = separator.getOrElse(detectSeparator(path))
    var reader = sparkSession.read.option("sep", sep).option("header", header.toString)
    schema.foreach(reader.schema(_))
    reader.csv(path)
  }

  def readHiveTable(tableName: String): DataFrame = {
    sparkSession.table(tableName)
  }

  def readParquet(path: String, schema: Option[StructType] = None): DataFrame = {
    var reader = sparkSession.read
    schema.foreach(reader.schema(_))
    reader.parquet(path)
  }

  override def read(path: String): DataFrame = {
    readCsv(path)
  }

  override def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }

}
