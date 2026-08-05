package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.models._
import org.apache.spark.sql.expressions.Window

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale


class DataTransformation(spark: SparkSession) {
  import spark.implicits._

  private def extractTimeFeatures(timestampStr: String): TimeFeatures = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val dt = LocalDateTime.parse(timestampStr, formatter)

    val hour = dt.getHour
    val dayOfWeek = dt.getDayOfWeek.getDisplayName(TextStyle.FULL, Locale.FRANCE)
    val month = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.FRANCE)
    val isWeekend = if (dt.getDayOfWeek.getValue >= 6) 1 else 0

    val dayPeriod = if (hour >= 6 && hour < 12) "Morning"
      else if (hour >= 12 && hour < 18) "Afternoon"
      else if (hour >= 18 && hour < 22) "Evening"
      else "Night"

    val isWorkingHour = if (hour >= 9 && hour <= 17) 1 else 0

    TimeFeatures(hour, dayOfWeek, month, isWeekend, dayPeriod, isWorkingHour)
  }

  // Enregistrement de l'UDF pour Spark
  private val extractTimeFeaturesUDF = udf(extractTimeFeatures _)

  def enrichTransactionData(
                             merchants: Dataset[Merchant],
                             transactions: Dataset[Transaction],
                             users: Dataset[User],
                             products: Dataset[Product]
                           ): DataFrame = {

    // Renommer les colonnes pour éviter les conflits de noms
    val productsClean = products.withColumnRenamed("category", "product_category")
    val merchantsClean = merchants.withColumnRenamed("category", "merchant_category")

    val dfJoined = transactions.join(users, Seq("user_id"), "inner")
      .join(merchantsClean, Seq("merchant_id"), "inner")
      .join(productsClean, Seq("product_id"), "inner")

    // Créer une colonne temporaire 'features' puis l'éclater
    val dfWithTime = dfJoined
      .withColumn("time_features", extractTimeFeaturesUDF(col("timestamp")))
      .select(col("*"), col("time_features.*")) // Le .* transforme la struct en colonnes
      .drop("time_features")

    // Les Window Functions
    val userWindow = Window.partitionBy("user_id").orderBy("timestamp")
    val userGlobalWindow = Window.partitionBy("user_id") // Fenêtre sans ordre pour le total

    val dfFinal = dfWithTime
      .withColumn("transaction_rank", rank().over(userWindow))
      .withColumn("total_transaction_by_user", count("transaction_id").over(userGlobalWindow))
      .withColumn("age_group",
        when(col("age") <= 25, "Jeune")
          .when(col("age") >= 26 && col("age") <= 44, "Adulte")
          .when(col("age") >= 45 && col("age") <= 64, "Age Moyen")
          .otherwise("Senior")
      )

    dfFinal
  }
}
