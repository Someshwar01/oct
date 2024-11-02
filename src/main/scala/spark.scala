
  import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import org.apache.spark.implicits._
import org.apache.spark.sql.functions._

object spark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()


    val df = spark.read
      .format("csv")
      .option("header", false)
      .option("path", "C:/Users/anike/Desktop/spark/education.csv")
      .load()


    df.show()
  }
}


