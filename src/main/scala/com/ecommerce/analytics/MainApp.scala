import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("EcommerceAnalytics")
  .config("master", "local[*]")
  .getOrCreate()
