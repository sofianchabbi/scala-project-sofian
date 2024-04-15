package sofian.writer


import org.apache.spark.sql.DataFrame
import java.util.Properties

class Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    val props = new Properties()
    val propsStream = getClass.getResourceAsStream("/application.properties")
    props.load(propsStream)

    val format = props.getProperty("format", "csv")
    val separator = props.getProperty("separator", ";")

    df
      .write
      .option("header", "true")
      .option("sep", separator)
      .mode(mode)
      .format(format)
      .save(path)
  }

}
