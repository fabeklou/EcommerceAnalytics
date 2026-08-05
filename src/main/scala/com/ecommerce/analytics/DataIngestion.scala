package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import com.ecommerce.models._
import scala.util.Try

class DataIngestion(spark: SparkSession) {
  import spark.implicits._

  // Schéma Transaction (Explicite)
  private val transactionSchema = StructType(Array(
    StructField("transaction_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("product_id", StringType, nullable = false),
    StructField("merchant_id", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("location", StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  // Méthode générique de lecture avec gestion d'erreur (Question 2.3)
  // On utilise un paramètre 'readBlock' qui contient la logique de lecture spécifique
  private def safeRead[T](datasetName: String)(readBlock: => Dataset[T]): Dataset[T] = {
    try {
      val ds = readBlock
      // On force une action (count) pour déclencher la lecture et capturer l'erreur ici
      val totalLines = ds.count()
      println(s"✅ [SUCCESS] $datasetName chargé : $totalLines lignes lues.")
      ds
    } catch {
      case e: Exception =>
        println(s"⛔ [ERROR] Erreur lors de la lecture de $datasetName : ${e.getMessage}")
        // En cas d'erreur, on renvoie un Dataset vide du même type pour ne pas bloquer tout le pipeline
        spark.emptyDataset[T]
    }
  }

  // --- Implémentation des méthodes de lecture ---

  def readTransactions(path: String): Dataset[Transaction] = safeRead("Transactions") {
    spark
      .read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(path)
      .as[Transaction]
  }

  def readUsers(path: String): Dataset[User] = safeRead("Users") {
    spark
      .read
      .json(path)
      .as[User]
  }

  def readProducts(path: String): Dataset[Product] = safeRead("Products") {
    spark
      .read
      .parquet(path)
      .as[Product]
  }

  def readMerchants(path: String): Dataset[Merchant] = safeRead("Merchants") {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[Merchant]
  }

  // --- Implémentation des méthodes de validation des données ---

  def validateTransactions(ds: Dataset[Transaction]): Dataset[Transaction] = {
    val validDs = ds.filter(t => t.amount > 0 && t.timestamp.length == 14)
    println(s"📊 Transactions valides : ${validDs.count()} lignes.")
    validDs
  }

  def validateUsers(ds: Dataset[User]): Dataset[User] = {
    val validDs = ds.filter(u => u.age >= 16 && u.age <= 100 && u.annual_income > 0)
    println(s"📊 Users valides : ${validDs.count()} lignes.")
    validDs
  }

  def validateProducts(ds: Dataset[Product]): Dataset[Product] = {
    val validDs = ds.filter(p => p.price > 0 && p.rating >= 1 && p.rating <= 5)
    println(s"📊 Products valides : ${validDs.count()} lignes.")
    validDs
  }

  def validateMerchants(ds: Dataset[Merchant]): Dataset[Merchant] = {
    val validDs = ds.filter(u => u.commission_rate >= 0 && u.commission_rate <= 1)
    println(s"📊 Merchants valides : ${validDs.count()} lignes.")
    validDs
  }
}
