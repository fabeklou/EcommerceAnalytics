package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.models._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale

// 1. On déplace la logique de l'UDF dans un OBJET pour la rendre sérialisable
object DataTransformation {

  // --- LOGIQUE INTERNE (UDF) ---

  private def extractTimeFeaturesLogic(timestampStr: String): TimeFeatures = {
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

  // Enregistrement de l'UDF (maintenant accessible sans transporter la SparkSession)
  val extractTimeFeaturesUDF = udf(extractTimeFeaturesLogic _)
}

// 2. La CLASSE conserve l'orchestration des données
class DataTransformation(spark: SparkSession) {
  import spark.implicits._
  import DataTransformation._ // On importe l'UDF depuis l'objet ci-dessus

  // --- MÉTHODES DE TRANSFORMATION ---

  /**
   * Joint les datasets et applique les enrichissements de base (UDF, Tranches d'âge, Rang)
   */
  def enrichTransactionData(
                             merchants: Dataset[Merchant],
                             transactions: Dataset[Transaction],
                             users: Dataset[User],
                             products: Dataset[Product]
                           ): DataFrame = {

    // Renommer les colonnes pour éviter les conflits de noms
    val productsClean = products
      .withColumnRenamed("category", "product_category")
      .withColumnRenamed("name", "product_name")
      .drop("merchant_id")  // ON SUPPRIME le merchant_id du produit car il crée une ambiguïté
    val merchantsClean = merchants
      .withColumnRenamed("category", "merchant_category")
      .withColumnRenamed("name", "merchant_name")

    // Optimisation des jointures avec broadcast sur les petits df
    val dfJoined = transactions
      .join(broadcast(users), Seq("user_id"), "inner")
      .join(broadcast(merchantsClean), Seq("merchant_id"), "inner")
      .join(broadcast(productsClean), Seq("product_id"), "inner")

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

  /**
   * Ajoute des analyses glissantes (Rolling Windows) pour le montant et l'activité utilisateur
   */
  def addRollingAnalytics(df: DataFrame): DataFrame = {
    // Définir la fenêtre glissante de 7 jours :
    // On transforme le String yyyyMMddHHmmss en secondes pour pouvoir faire des maths
    val secondsIn7Days = 7 * 24 * 60 * 60
    val timeCol = unix_timestamp(col("timestamp"), "yyyyMMddHHmmss")

    // Créer une colonne 'date' pure (sans l'heure) pour compter les jours
    val dfWithDate = df.withColumn("date_only", to_date(col("timestamp"), "yyyyMMddHHmmss"))

    val rollingWindow = Window.partitionBy("user_id")
      .orderBy(timeCol)
      .rangeBetween(-secondsIn7Days, 0) // Regarde 7 jours en arrière jusqu'à la ligne actuelle

    // Ajouter les colonnes (montant et l'ensemble des jours uniques)
    val dfWithRolling = dfWithDate
      .withColumn("cumulated_amount_7d", sum("amount").over(rollingWindow))
      .withColumn("distinct_days_set", collect_set("date_only").over(rollingWindow))
      .withColumn("nb_active_days", size(col("distinct_days_set")))

    // Calculer et ajouter le flag final
    dfWithRolling.withColumn("is_active_user",
      when(col("nb_active_days") >= 5, 1).otherwise(0)
    ).drop("date_only", "distinct_days_set", "nb_active_days") // Nettoyer les colonnes temporaires
  }
}
