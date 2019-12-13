import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/***
 * This is an example of spark word count.
 * Replace the ??? with appropriate implementation and run this file
 * with the argument "input/ScalaWiki.txt"
 * Write down the count of word "Scala" in your submission
 * You can find the implementation from https://spark.apache.org/examples.html
 */

object Movie extends App {

  def getRes(df: DataFrame) = df
    .groupBy("movieId")
    .agg(mean("rating"), stddev_pop("rating"))
    .orderBy("movieId")

  override def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Movie")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header","true")
      .option("inferschema","true")
      .csv("src/data/rating.csv")

    getRes(df).show
  }
}