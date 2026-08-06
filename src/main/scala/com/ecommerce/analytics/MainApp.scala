package com.ecommerce.analytics

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MainApp {
  def main(args: Array[String]): Unit = {
    // CHARGEMENT DE LA CONFIGURATION
    val config = ConfigFactory.load()
    val appName = config.getString("app.name")
    val input = config.getConfig("app.data.input")
    val outputPath = config.getString("app.data.output.path")

    // INITIALISATION DE LA SESSION SPARK
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]") // Utilisation de tous les coeurs disponibles
      .getOrCreate()

    try {
      // Instanciation des modules logiques
      val ingestion = new DataIngestion(spark)
      val transformer = new DataTransformation(spark)
      val analytics = new Analytics(spark)

      // PHASE D'INGESTION ET VALIDATION
      println("🚀 ETAPE 1 : Ingestion et Validation des données...")

      val merchants = ingestion.readMerchants(input.getString("merchants"))
      val validMerchants = ingestion.validateMerchants(merchants)

      val products = ingestion.readProducts(input.getString("products"))
      val validProducts = ingestion.validateProducts(products)

      val users = ingestion.readUsers(input.getString("users"))
      val validUsers = ingestion.validateUsers(users)

      val transactions = ingestion.readTransactions(input.getString("transactions"))
      val validTransactions = ingestion.validateTransactions(transactions)

      // PHASE DE TRANSFORMATION AVANCÉE
      println("⚙️ ETAPE 2 : Enrichissement et calculs Window Functions...")

      // Jointures, UDF temporelle et tranches d'âge
      val enrichedBase = transformer.enrichTransactionData(
        validMerchants,
        validTransactions,
        validUsers,
        validProducts
      )

      // Analyses glissantes (Montant cumulé et Utilisateur Actif sur 7 jours)
      val finalEnriched = transformer.addRollingAnalytics(enrichedBase)

      // OPTIMISATION DU STOCKAGE
      // On persiste car ce DataFrame est utilisé pour deux rapports différents
      finalEnriched.persist(StorageLevel.MEMORY_AND_DISK_SER)
      finalEnriched.count() // cache warming

      // PHASE ANALYTIQUE BUSINESS
      println("📊 ETAPE 3 : Génération des rapports KPIs...")

      val merchantReport = analytics.generateMerchantReport(finalEnriched)
      val cohortReport = analytics.cohortsAnalysis(finalEnriched)

      // AFFICHAGE ET SAUVEGARDE

      // Affichage console pour vérification
      println("--- RAPPORT MARCHANDS (Top 20) ---")
      merchantReport.show(20)

      println("--- ANALYSE DE COHORTES ---")
      cohortReport.show(20)

      // Sauvegarde multi-formats
      println(s"💾 Sauvegarde des résultats dans $outputPath ...")

      merchantReport.write.mode("overwrite")
        .parquet(s"$outputPath/merchant_report_parquet")

      merchantReport.write.mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/merchant_report_csv")

      cohortReport.write.mode("overwrite")
        .parquet(s"$outputPath/cohort_report_parquet")

      cohortReport.write.mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/cohort_report_csv")

      // NETTOYAGE
      finalEnriched.unpersist()
      println("✅ Pipeline EcommerceAnalytics terminé avec succès !")

    } catch {
      case e: Exception =>
        println(s"💥 ERREUR CRITIQUE détectée dans le pipeline : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
