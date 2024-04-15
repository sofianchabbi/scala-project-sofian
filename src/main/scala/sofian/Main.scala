package sofian

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sofian.processor.impl.ProcessorImpl
import sofian.reader.impl.ReaderImpl
import sofian.writer.Writer

object Main extends App {
  val cliArgs = args
  val MASTER_URL: String = cliArgs.headOption.getOrElse("local[1]")
  val SRC_PATH: String = cliArgs.lift(1).getOrElse {
    println("No input path defined")
    sys.exit(1)
  }

  val conf: SparkConf = new SparkConf()
  conf.set("spark.driver.memory", "1G")
  conf.set("spark.testing.memory", "471859200")


  val DST_PATH1: String = cliArgs.lift(2).getOrElse("./default/report1")
  val DST_PATH2: String = cliArgs.lift(3).getOrElse("./default/output-writer2")
  val DST_PATH3: String = cliArgs.lift(4).getOrElse("./default/output-writer3")

  // Configuration de SparkSession
  val sparkSession = SparkSession.builder()
    .appName("Rapports de Rappel de Produits")
    .config(conf)
    .master(MASTER_URL)
    .getOrCreate()

  // Initialisation des instances de Reader, Processor et Writer
  val reader = new ReaderImpl(sparkSession)
  val processor = new ProcessorImpl(sparkSession)
  val writer = new Writer()

  val srcPath = SRC_PATH
  val dstPath1 = DST_PATH1
  val dstPath2 = DST_PATH2
  val dstPath3 = DST_PATH3

  val inputData = reader.read("csv", Map("header" -> "true", "inferSchema" -> "true"), srcPath)
  inputData.printSchema()

  // Génération des rapports
  val report1 = processor.generateRecallReasonsReport(inputData)
  val report2 = processor.generateVersionReport(inputData)
  val report3 = processor.generateRiskAndRecommendationsReport(inputData)

  // Écriture des rapports
  writer.write(report1, "overwrite", dstPath1)
  writer.write(report2, "overwrite", dstPath2)
  writer.write(report3, "overwrite", dstPath3)

  // Fermeture de la session Spark
  sparkSession.stop()
}


