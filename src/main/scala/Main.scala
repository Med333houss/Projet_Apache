import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Missing two arguments.")
      println("usage : You have to give the source file path and the output path")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder
      .appName("ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    try {
      // Pipeline ETL
      val rawData = Extract.loadData(spark, inputPath)
      val cleanedData = Transform.cleanData(rawData)
      val finalData = Transform.computeTrafficRevenue(cleanedData)
      Load.saveData(finalData, outputPath)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}