package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App{

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/bands.json")
  /**
    * bands =>
    * {"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}
    * {"id":0,"name":"Led Zeppelin","hometown":"London","year":1968}
    * {"id":3,"name":"Metallica","hometown":"Los Angeles","year":1981}
    * {"id":4,"name":"The Beatles","hometown":"Liverpool","year":1960}
    */

  val guitarPlayerDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitarPlayers.json")
  /**
    * guitarPlayers =>
    * {"id":0,"name":"Jimmy Page","guitars":[0],"band":0}
    * {"id":1,"name":"Angus Young","guitars":[1],"band":1}
    * {"id":2,"name":"Eric Clapton","guitars":[1,5],"band":2}
    * {"id":3,"name":"Kirk Hammett","guitars":[3],"band":3}
    */

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")
  /**
    * {"id":0,"model":"EDS-1275","make":"Gibson","guitarType":"Electric double-necked"}
    * {"id":5,"model":"Stratocaster","make":"Fender","guitarType":"Electric"}
    * {"id":1,"model":"SG","make":"Gibson","guitarType":"Electric"}
    * {"id":2,"model":"914","make":"Taylor","guitarType":"Acoustic"}
    * {"id":3,"model":"M-II","make":"ESP","guitarType":"Electric"}
    */

  // inner Joins (condition by default)
  val joinCondition = guitarPlayerDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitarPlayerDF.join(bandsDF,joinCondition)

  /**
    * guitaristsBandsDF
    * +----+-------+---+------------+-----------+---+------------+----+
    * |band|guitars| id|        name|   hometown| id|        name|year|
    * +----+-------+---+------------+-----------+---+------------+----+
    * |   1|    [1]|  1| Angus Young|     Sydney|  1|       AC/DC|1973|
    * |   0|    [0]|  0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
    * |   3|    [3]|  3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
    * +----+-------+---+------------+-----------+---+------------+----+
    */


  /**
    * OUTER JOINS
    */


  // left_outer = everything in the inner join + all the rows in the LEFT table,
  // with nulls in where the data missing

  guitarPlayerDF.join(bandsDF,joinCondition,"left_outer")

  /**
    * +----+-------+---+------------+-----------+----+------------+----+
    * |band|guitars| id|        name|   hometown|  id|        name|year|
    * +----+-------+---+------------+-----------+----+------------+----+
    * |   0|    [0]|  0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
    * |   1|    [1]|  1| Angus Young|     Sydney|   1|       AC/DC|1973|
    * |   2| [1, 5]|  2|Eric Clapton|       null|null|        null|null|
    * |   3|    [3]|  3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
    * +----+-------+---+------------+-----------+----+------------+----+
    */


  // right_outer = everything in the inner join + all the rows in the RIGHT table,
  // with nulls in where the data missing

  guitarPlayerDF.join(bandsDF,joinCondition,"right_outer")

  /**
    * +----+-------+----+------------+-----------+---+------------+----+
    * |band|guitars|  id|        name|   hometown| id|        name|year|
    * +----+-------+----+------------+-----------+---+------------+----+
    * |   1|    [1]|   1| Angus Young|     Sydney|  1|       AC/DC|1973|
    * |   0|    [0]|   0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
    * |   3|    [3]|   3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
    * |null|   null|null|        null|  Liverpool|  4| The Beatles|1960|
    * +----+-------+----+------------+-----------+---+------------+----+
    */

  // outer = everything in the inner join + all the rows in BOTH tables, with nulls in where data missing
  guitarPlayerDF.join(bandsDF,joinCondition,"outer")

  /**
    * +----+-------+----+------------+-----------+----+------------+----+
    * |band|guitars|  id|        name|   hometown|  id|        name|year|
    * +----+-------+----+------------+-----------+----+------------+----+
    * |   0|    [0]|   0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
    * |   1|    [1]|   1| Angus Young|     Sydney|   1|       AC/DC|1973|
    * |   3|    [3]|   3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
    * |   2| [1, 5]|   2|Eric Clapton|       null|null|        null|null|
    * |null|   null|null|        null|  Liverpool|   4| The Beatles|1960|
    * +----+-------+----+------------+-----------+----+------------+----+
    */


  //semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitarPlayerDF.join(bandsDF, joinCondition, "left_semi")

  /**
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   0|    [0]|  0|  Jimmy Page|
    * |   1|    [1]|  1| Angus Young|
    * |   3|    [3]|  3|Kirk Hammett|
    * +----+-------+---+------------+
    */

  //anti_join = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitarPlayerDF.join(bandsDF, joinCondition, "left_anti")
  /**
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   2| [1, 5]|  2|Eric Clapton|
    * +----+-------+---+------------+
    */

  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitarPlayerDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitarPlayerDF.join(bandsModDF, guitarPlayerDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitarPlayerDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)"))






  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  /**
    * root
    * |-- emp_no: integer (nullable = true)
    * |-- birth_date: date (nullable = true)
    * |-- first_name: string (nullable = true)
    * |-- last_name: string (nullable = true)
    * |-- gender: string (nullable = true)
    * |-- hire_date: date (nullable = true)
    */

  val salariesDF = readTable("salaries")
  /**
    * root
    * |-- emp_no: integer (nullable = true)
    * |-- salary: integer (nullable = true)
    * |-- from_date: date (nullable = true)
    * |-- to_date: date (nullable = true)
    */

  val deptManagersDF = readTable("dept_manager")
  /**
    * root
    * |-- dept_no: string (nullable = true)
    * |-- emp_no: integer (nullable = true)
    * |-- from_date: date (nullable = true)
    * |-- to_date: date (nullable = true)
    */

  val titlesDF = readTable("titles")
  /**
    * root
    * |-- emp_no: integer (nullable = true)
    * |-- title: string (nullable = true)
    * |-- from_date: date (nullable = true)
    * |-- to_date: date (nullable = true)
    */

  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no")
    .agg(max("salary").as("maxSalary"))
  val empMaxSalary = employeesDF.join(maxSalariesPerEmpNoDF,"emp_no")

  val empNeverManagers = employeesDF
    .join(deptManagersDF, Seq("emp_no"), "left_anti")

  val meanPaidPerTitle = titlesDF.join(salariesDF,"emp_no")
    .groupBy("title").agg(avg("salary")
    .as("average_salary_per_title"))
    .orderBy(desc("average_salary_per_title")).limit(10)

  meanPaidPerTitle.show()

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = salariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")
}
