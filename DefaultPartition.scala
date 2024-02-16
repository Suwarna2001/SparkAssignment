import org.apache.spark.sql.{SparkSession, DataFrame}

object DefaultPartition {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("dataframepartition")
      .master("local[*]") // Run locally using all available cores
      .getOrCreate()

    // Sample data for demonstration
    val data = Seq(
      ("A", 34),
      ("B", 45),
      ("C", 28),
      ("D", 56),
      ("E", 40)
    )

    // Create a DataFrame from the sample data
    import spark.implicits._
    val df = data.toDF("Name", "Age")

    // Check the number of partitions before shuffle
    val initialPartitions = df.rdd.getNumPartitions
    println(s"Number of partitions before shuffle: $initialPartitions")

    // Perform a shuffle operation (group by)
    val groupedDF: DataFrame = df.groupBy("Age").count()

    // Check the number of partitions after shuffle
    val partitionsAfterShuffle = groupedDF.rdd.getNumPartitions
    println(s"Number of partitions after shuffle: $partitionsAfterShuffle")

    // Show the result
    groupedDF.show()

    val delayInMilliseconds = 5 * 60 * 1000 // 5 minutes in milliseconds
    println(s"Pausing execution for ${delayInMilliseconds / 1000 / 60} minutes before shutting down Spark session...")
    Thread.sleep(delayInMilliseconds)

    // Stop SparkSession
    spark.stop()
  }
}


