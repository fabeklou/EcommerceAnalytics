package com.ecommerce.models

// Modèle pour les marchands (merchants.csv)
case class Merchant(
                     merchant_id: String,
                     name: String,
                     category: String,
                     region: String,
                     commission_rate: Double,
                     establishment_date: String // Format: yyyyMMdd
                   )

// Modèle pour les transactions (transactions.csv)
case class Transaction(
                        transaction_id: String,
                        user_id: String,
                        product_id: String,
                        merchant_id: String,
                        amount: Double,
                        timestamp: String, // Format: yyyyMMddHHmmss
                        location: String,
                        payment_method: String,
                        category: String
                      )

// Modèle pour les utilisateurs (users.json)
case class User(
                 user_id: String,
                 age: Int,
                 annual_income: Double,
                 city: String,
                 customer_segment: String,
                 preferred_categories: Array[String], // Champ imbriqué (JSON Array)
                 registration_date: String // Format: yyyyMMdd
               )

// Modèle pour les produits (products.parquet)
case class Product(
                    product_id: String,
                    name: String,
                    category: String,
                    price: Double,
                    merchant_id: String,
                    rating: Double,
                    stock: Int
                  )

// Modèle des caractéristiques temporelles enrichies à partir d'un timestamp
case class TimeFeatures(
                         hour: Int,
                         day_of_week: String,
                         month: String,
                         is_weekend: Int,
                         day_period: String,
                         is_working_hours: Int
                       )
