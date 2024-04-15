package sofian.processor


import org.apache.spark.sql.DataFrame

trait Processor {
  def generateRecallReasonsReport(df: DataFrame): DataFrame
  def generateVersionReport(df: DataFrame): DataFrame
  def generateRiskAndRecommendationsReport(df: DataFrame): DataFrame
}
