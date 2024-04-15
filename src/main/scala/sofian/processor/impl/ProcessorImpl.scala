package sofian.processor.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import sofian.processor.Processor

class ProcessorImpl(sparkSession: SparkSession) extends Processor {

  def generateRecallReasonsReport(df: DataFrame): DataFrame = {
    df.groupBy("categorie_de_produit", "motif_du_rappel")
      .count()
      .orderBy(desc("count"))
  }

  def generateVersionReport(df: DataFrame): DataFrame = {
    /*df.withColumn("version_range",
        when(col("ndge_de_version") <= 1, "Inférieure à 1")
          .when(col("ndge_de_version").between(2, 4), "Entre 2 et 4")
          .otherwise("Supérieure à 4"))
      .groupBy("version_range")
      .count()
      .orderBy("version_range")*/

    df.groupBy("categorie_de_produit", "motif_du_rappel")
      .count()
      .orderBy(desc("count"))
  }

  override def generateRiskAndRecommendationsReport(df: DataFrame): DataFrame = {
    df.select("risques_encourus_par_le_consommateur", "preconisations_sanitaires")
      .groupBy("risques_encourus_par_le_consommateur")
      .agg(
        count("*").alias("Nombre_de_risques_encourus"),
        countDistinct("preconisations_sanitaires").alias("Nombre_de_preconisations_distinctes")
      )
      .orderBy("risques_encourus_par_le_consommateur")
  }
}
