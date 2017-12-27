package book_spark_in_action.ch3

import org.apache.spark.sql.SparkSession

import scala.io.Source

object Main extends App{

  val spark = SparkSession.builder()
    .appName("GitHub push counter")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val homeDir = System.getenv("HOME")
  val inputPath = homeDir + "/dev/books/first-edition-dataset/*.json"
  val ghLog = spark.read.json(inputPath)

  val pushes = ghLog.filter("type = 'PushEvent'")
  pushes.printSchema()
  println(s"all events: ${ghLog.count()}")
  println(s"only pushes: ${pushes.count()}")
  pushes.show(5)

  val grouped = pushes.groupBy("actor.login").count
  val ordered = grouped.orderBy(grouped("count").desc)
  ordered.show(5)

  val empPath = homeDir + "/Dev/books/first-edition/ch03/ghEmployees.txt"
  val employees = Set() ++ {
    for {
      line <- Source.fromFile(empPath).getLines
    } yield line.trim
  }
  val bcEmployees = sc.broadcast(employees)

  import spark.implicits._
  def isEmp(user: String) = bcEmployees.value.contains(user)
  def isEmployees = spark.udf.register("SetContainsUdf", isEmp _)
  val filtered = ordered.filter(isEmployees($"login"))
  filtered.show()

}
