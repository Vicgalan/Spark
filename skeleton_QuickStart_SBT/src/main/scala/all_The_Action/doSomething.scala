import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.functions._

class doSomething {

  // Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  //create a SparkSession using every core of the local machine
  val spark = sql.SparkSession
    .builder
    .appName("NAME")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def youWant() = {
    println("QuickStar")
  }

}
