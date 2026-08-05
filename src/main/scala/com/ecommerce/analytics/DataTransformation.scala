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

  def generateMerchantReport(enrichedDf: DataFrame): DataFrame = {
    // Rapport détaillé par marchand

    val baseMetrics = enrichedDf.groupBy(
        "merchant_id",
        "name",
        "merchant_category",
        "region",
        "commission_rate"
      )
      .agg(
        sum("amount").as("total_turnover"),
        count("transaction_id").as("num_of_transactions"),
        countDistinct("user_id").as("num_of_unique_clients"),
        avg("amount").as("avg_transaction_amount")
      )

    // Classement par CA (CA descendant) dans la catégorie et la région
    val rankWindow = Window
      .partitionBy("merchant_category", "region")
      .orderBy(desc("total_turnover"))

    val dfWithRank = baseMetrics.withColumn("rank_in_category_region", rank().over(rankWindow))

    // Calcul de la commission totale
    val dfWithCommission = dfWithRank.withColumn(
      "total_commission_earned",
      col("total_turnover") * col("commission_rate")
    )

    // Répartition des ventes par tranche d'âge (Le Pivot)
    // On crée un petit DataFrame à part qui calcule le CA par marchand et par âge
    val ageDistribution = enrichedDf.groupBy("merchant_id")
      .pivot("age_group") // Ceci va créer des colonnes "Jeune", "Adulte", etc.
      .sum("amount")
      .na.fill(0) // On remplace les nulls par 0 si un marchand n'a pas de ventes pour une catégorie d'âge

    // Jointure finale pour avoir le rapport complet
    val finalReport = dfWithCommission.join(ageDistribution, Seq("merchant_id"), "left")

    finalReport
  }

  def cohortsAnalysis(enrichedDf: DataFrame): DataFrame = {

    // Définir une fenêtre par utilisateur pour trouver sa toute première date
    val userWindow = Window.partitionBy("user_id")

    // Ajouter la date de première transaction sur CHAQUE ligne
    val dfWithFirstDate = enrichedDf.withColumn(
      "first_transaction_date",
      min("timestamp").over(userWindow)
    )

    // Convertir cette date en mois de cohorte (ex: "2024-07")
    val dfWithCohort = dfWithFirstDate.withColumn(
      "cohort_month",
      date_format(to_date(col("first_transaction_date"), "yyyyMMddHHmmss"), "yyyy-MM")
    )

    // Rapport final : Agrégation par mois de cohorte
    dfWithCohort.groupBy("cohort_month")
      .agg(
        countDistinct("user_id").as("num_users_in_cohort"),
        count("transaction_id").as("total_transactions"),
        sum("amount").as("total_revenue_generated"),
        avg("amount").as("avg_revenue")
      )
      .orderBy("cohort_month")
  }
}
