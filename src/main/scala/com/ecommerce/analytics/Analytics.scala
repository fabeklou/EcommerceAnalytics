package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Analytics(spark: SparkSession) {
  import spark.implicits._

  /**
   * Calcule les KPIs détaillés par marchand incluant CA, rang, commission et pivot par âge
   */
  def generateMerchantReport(enrichedDf: DataFrame): DataFrame = {
    // Rapport détaillé par marchand

    val baseMetrics = enrichedDf.groupBy(
        "merchant_id",
        "merchant_name",
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

  /**
   * Identifie les cohortes d'utilisateurs basées sur leur mois de première transaction
   */
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
