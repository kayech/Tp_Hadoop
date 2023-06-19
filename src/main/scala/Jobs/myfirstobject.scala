package Jobs

import org.apache.spark.sql.SparkSession

object myfirstobject extends App {
  val Job = "Mon_premier_Job_IntelliJ"

  val spark = SparkSession
    .builder()
    .appName(Job)
    .enableHiveSupport()
  .getOrCreate()


  import spark.implicits._

  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))

  val rdd = spark.sparkContext.parallelize(dataSeq)

  println("Nombre initial de partitions :" + rdd.getNumPartitions)

  println("Collect :" + rdd.collect())

  println("Premier element :" + rdd.first())

  val columns = Seq("language", "users_count")

  val dfFromRDD1 = rdd.toDF(columns: _*)

  dfFromRDD1.show


}
