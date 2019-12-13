import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class MovieSpec extends FlatSpec with Matchers {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Movie")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df_test = Seq(
    (1, 3.9), (1, 5.0), (2, 3.5), (2, 2.8), (2, 4.2),
    (8, 4.3), (15, 4.8), (15, 3.9), (15, 4.5), (15, 3.6)
  ).toDF("movieId", "rating")

  val x = Movie.getRes(df_test)

  behavior of "getRes"

  it should "get correct movieIds" in {
    x.select("movieId").rdd.map(r => r(0)).collect() shouldBe (Array(1, 2, 8, 15))
  }

  it should "get correct means" in {
    val l_mean = x.select("avg(rating)").rdd.map(r => r(0)).collect().toList.map(_.toString.toDouble)
    val l_temp = List(4.45, 3.5, 4.3, 4.2).zip(l_mean).map(t => math.abs(t._1 - t._2))
    l_temp.max < 0.001 shouldBe(true)
  }

  it should "get correct stds" in {
    val l_std = x.select("stddev_pop(rating)").rdd.map(r => r(0)).collect().toList.map(_.toString.toDouble)
    val l_temp = List(0.55, 0.5715476066, 0, 0.474341649).zip(l_std).map(t => math.abs(t._1 - t._2))
    l_temp.max < 0.001 shouldBe(true)
  }

  it should "get correct dataframe" in {
    x.columns.size shouldBe 3
    x.count() shouldBe 4
  }
}
