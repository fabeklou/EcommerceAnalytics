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

  def addRollingAnalytics(df: DataFrame): DataFrame = {
    // Définir la fenêtre glissante de 7 jours :
    // On transforme le String yyyyMMddHHmmss en secondes pour pouvoir faire des maths
    val secondsIn7Days = 7 * 24 * 60 * 60
    val timeCol = unix_timestamp(col("timestamp"), "yyyyMMddHHmmss")

    val rollingWindow = Window.partitionBy("user_id")
      .orderBy(timeCol)
      .rangeBetween(-secondsIn7Days, 0) // Regarde 7 jours en arrière jusqu'à la ligne actuelle

    // Ajouter les colonnes (montant et nombre de transactions sur 7 Jours)
    val dfWithRolling = df
      .withColumn(
        "cumulated_amount_7d",
        sum("amount").over(rollingWindow)
      ).withColumn(
        "nb_transactions_7d",
        count("transaction_id").over(rollingWindow)
      )

    // Calculer et ajouter le flag final
    dfWithRolling.withColumn("is_active_user",
      when(col("nb_transactions_7d") >= 5, 1).otherwise(0)
    )
  }
}
