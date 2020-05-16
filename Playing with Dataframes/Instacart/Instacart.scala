import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{sum, col}

object Instacart extends App {

  // Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  //create a SparkSession using every core of the local machine
  val spark = sql.SparkSession
    .builder
    .appName("Instacart")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  /** 1.Load data into Spark DataFrame */

  val aisles = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter",",")
    .load("src/data/aisles.csv")

  val departments = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter",",")
    .load("src/data/departments.csv")

  val order_products_train = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter",",")
    .load("src/data/order_products__train.csv")

  val orders = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter",",")
    .load("src/data/orders.csv")

  val products = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter",",")
    .load("src/data/products.csv")



  /** 2. Merge all the data frames based on the common key and create a single DataFrame  */

  val instacart_data = aisles
    .join(products,Seq("aisle_id"),"inner")
    .join(departments,Seq("department_id"),"inner")
    .join(order_products_train, Seq("product_id"),"inner")
    .join(orders,Seq("order_id"),"inner")

  //instacart_data.show()

  /** 3. Check missing data */

  val missing_data = instacart_data
    .select(instacart_data.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*)

  /** 4. List the most ordered products (top 10)  */

  val product_top10 = instacart_data.groupBy("product_id","product_name").count()
    .withColumnRenamed("count","Total_orders")
    .orderBy(desc("Total_orders")).limit(10)

  /** 5. Do people usually reorder the same previous ordered products? */

  instacart_data.agg(avg("reordered")).show(false)

  /** 6. List most reordered products  */

  instacart_data.groupBy("product_name").agg(avg("reordered")*100,count("reordered"))
    .withColumnRenamed("avg(reordered)","avg_reordered")
    .withColumnRenamed("count(reordered)","total_reordered")
    .orderBy(desc("total_reordered")).show(10,false)

  /** 7. Most important aisle (by number of products) */

  instacart_data.groupBy("aisle").count.sort($"count".desc).show(10,false)

  /** 8. Get the Top 10 departments  */

  instacart_data.groupBy("department").count().withColumnRenamed("count","Total_orders")
    .orderBy(desc("Total_orders")).limit(10).show(false)

  /** 9. List top 10 products ordered in the morning (6 AM to 11 AM) */

  instacart_data.filter(col("order_hour_of_day") >= 6 and col("order_hour_of_day") <= 11)
    .groupBy("product_name").count().withColumnRenamed("count","Total_orders")
    .orderBy(desc("Total_orders")).limit(10).show(false)

}
