import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main extends App {

  val conf: SparkConf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
   conf.set("spark.testing.memory", "471859200")

  private val sparkSession = SparkSession
    .builder
    .master("local[1]")
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  print(sparkSession.sql("SELECT 'A'").show())
}