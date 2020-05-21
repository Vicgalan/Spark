import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object mobileAppStore extends App {

  // Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  //create a SparkSession using every core of the local machine
  val spark = SparkSession
    .builder
    .appName("APP_NAME")
    .master("local[*]")
    .getOrCreate()

  // this is used to implicitly convert an RDD to a DataFrame.
  import spark.implicits._

  /** 1. Load csv into spark as a text file */

  val lines = spark.sparkContext.textFile("src/data/AppleStore_sep2.csv")

  /** 2. Parse the data as csv */

  /** • Create Scala case class to parse the file */

  case class mobileApp(id: Long,track_name: String, size_bytes: Long,
                       currency: String, price: Double, rating_count_tot: Int,
                       rating_count_ver: Int, user_rating:Double, user_rating_ver:Double,
                       ver:String, cont_rating:String, prime_genre:String,screenshots:Int,
                       ipadSc_urls_num:Int,	lang_num:Int,vpp_lic:Int)

  /** • Split the file and parse it*/

  def mapper(line : String): mobileApp = {

    val fields = line.split(";")

    val mobile_App_Store: mobileApp = mobileApp(fields(1).toLong, fields(2),fields(3).toLong,
      fields(4), fields(5).toDouble, fields(6).toInt,
      fields(7).toInt, fields(8).toDouble, fields(9).toDouble,
      fields(10), fields(11), fields(12), fields(13).toInt,
      fields(14).toInt, fields(15).toInt, fields(16).toInt)

    mobile_App_Store
  }

  val header = lines.first() //extract header
  val dataFileNoHeader = lines.filter(row => row != header)  //filter out header

  val mobile_App = dataFileNoHeader.map(mapper).toDF


  /** 3. Convert bytes to MB and GB in a new column */

  val App_Store = mobile_App
    .withColumn("size_MB",col("size_bytes")/1000000)
    .withColumn("size_GB",col("size_MB")/1000)

  /** 4. List top 10 trending apps */

  val top10 = App_Store.sort(desc("rating_count_tot")).limit(10)

  /** 5. Difference in the average number of screenshots displayed highest and
lowest rating apps*/

  val avg_highest = top10.agg(avg("screenshots")).first()(0).toString.toDouble

  val lowestTop10 = top10.agg(min("rating_count_tot")).first()(0)
  val avg_lowest = mobile_App.where(col("rating_count_tot") <= lowestTop10)
    .agg(avg("screenshots")).first()(0).toString.toDouble

  val avg_difference = avg_highest - avg_lowest

  /**  6. What percentage of high rated apps support multiple languages*/

  val multiple_language = top10.where(col("lang_num") > 1 ).count()*100/10

  /**  7. How does app details(price,lan_num, size) contribute to user ratings?

  For this we will be using rating percentiles and then will be checking the averages of
    details
  Get percentiles of ratings */

  val percentiles =
    App_Store.stat.approxQuantile("rating_count_tot",Array(0.25,0.50,0.75),0.0)

  /** Get the datasets with different percentiles*/

  val df_25 = App_Store.filter($"rating_count_tot" < percentiles(0))
  val df_50 = App_Store.filter($"rating_count_tot" >= percentiles(0) &&
    $"rating_count_tot" <  percentiles(1))
  val df_75 = App_Store.filter($"rating_count_tot" >= percentiles(1) &&
    $"rating_count_tot" <  percentiles(2))
  val df_100 = App_Store.filter($"rating_count_tot" >= percentiles(2))

  /** Compare the statistics */

  val details_25 = df_25.agg(avg("lang_num"),avg("price"),avg("size_MB"))
  val details_50 = df_50.agg(avg("lang_num"),avg("price"),avg("size_MB"))
  val details_75 = df_75.agg(avg("lang_num"),avg("price"),avg("size_MB"))
  val details_100 = df_100.agg(avg("lang_num"),avg("price"),avg("size_MB"))

  /** 8. Compare the statistics of different app groups/genres */

  val genres_analytics = App_Store.groupBy("prime_genre").agg(avg("lang_num"),avg("price"),
    avg("size_MB"),avg("rating_count_tot")).orderBy(desc("avg(rating_count_tot)"))

  /** 9. Does length of app description contribute to the ratings? */

  val app_description = App_Store.withColumn("length_description", length(col("track_name")))

  val app_description_25 = app_description.filter($"rating_count_tot" < percentiles(0))
  val app_description_50 = app_description.filter($"rating_count_tot" >= percentiles(0) &&
    $"rating_count_tot" <  percentiles(1))
  val app_description_75 = app_description.filter($"rating_count_tot" >= percentiles(1) &&
    $"rating_count_tot" <  percentiles(2))
  val app_description_100 = app_description.filter($"rating_count_tot" >= percentiles(2))

  val app_description_contribute_25 = app_description_25.agg(avg("length_description"))
    .withColumn("Percentil", lit("25"))

  val app_description_contribute_50 = app_description_50.agg(avg("length_description"))
    .withColumn("Percentil", lit("50"))

  val app_description_contribute_75 = app_description_75.agg(avg("length_description"))
    .withColumn("Percentil", lit("75"))

  val app_description_contribute_100 = app_description_100.agg(avg("length_description"))
    .withColumn("Percentil", lit("100"))

  val length_description = app_description_contribute_25.union(app_description_contribute_50)
    .union(app_description_contribute_75).union(app_description_contribute_100)

}
