package book_spark_in_action.ch3

import org.apache.spark.sql.SparkSession

import scala.io.Source

object Main2 extends App{

  val spark = SparkSession.builder().getOrCreate()

  val sc = spark.sparkContext

  val ghLog = spark.read.json(args(0))

  val pushes = ghLog.filter("type = 'PushEvent'")
  pushes.printSchema()
  println(s"all events: ${ghLog.count()}")
  println(s"only pushes: ${pushes.count()}")
  pushes.show(5)

  val grouped = pushes.groupBy("actor.login").count
  val ordered = grouped.orderBy(grouped("count").desc)
  ordered.show(5)

  val employees = Set() ++ {
    for {
      line <- Source.fromFile(args(1)).getLines
    } yield line.trim
  }
  val bcEmployees = sc.broadcast(employees)

  import spark.implicits._
  def isEmp(user: String) = bcEmployees.value.contains(user)
  def isEmployees = spark.udf.register("SetContainsUdf", isEmp _)
  val filtered = ordered.filter(isEmployees($"login"))

  filtered.write.format(args(3)).save(args(2))
}
