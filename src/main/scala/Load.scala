import org.apache.spark.sql.DataFrame

object Load {
  def saveData(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
  }
}
