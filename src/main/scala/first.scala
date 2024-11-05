import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//      import org.apache.spark.implicits._

import org.apache.spark.sql.functions._

import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger


object first {
  def main(args: Array[String]): Unit = {


    //    val spark = SparkSession.builder()
    //      .appName("spark-program")
    //      .master("local[*]")
    //      .getOrCreate()
    //
    //
    //    val df = spark.read
    //      .format("csv")
    //      .option("header", false)
    //      .option("path", "C:/Users/anike/Desktop/spark/education.csv")
    //      .load()
    //
    //
    //    df.show()


    //    val orderData = List(
    //      ("Order1", "John", 100),
    //      ("Order2", "Alice", 200),
    //      ("Order3", "Bob", 150),
    //      ("Order4", "Alice", 300),
    //      ("Order5", "Bob", 250),
    //      ("Order6", "John", 400)
    //    ).toDF("OrderID", "Customer", "Amount")
    //
    //
    //   orderData.groupBy("customer").agg(count(col("OrderID")),sum(col("Amount"))).show()


    val spark = SparkSession.builder()
      .appName("Spark_Program")
      .master("local[*]")
      .getOrCreate()

    //    import org.apache.spark.implicits._

    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("Name", StringType),
      StructField("Salary", IntegerType),
      StructField("City", StringType)
    ))
    //    val schema = " id Int, Name String, Salary Int, City String"


    val df = spark.read
      .format("csv")
      .option("header", true)
      .schema(schema)
      .option("path", "C:/Users/anike/Desktop/spark/details.csv")
      .load()

    //    df.printSchema()
    //    df.show()


    //        df.select(
    //          col("id"), col("Name"), col("Salary"), col("City"),
    //          when(
    //            col("Salary") >= 1001, "rich").when(col("salary")>=800 && col("salary")<=1000,"Middle").otherwise(("poor")).alias("Status")
    //        ).show()

    df.createTempView("Details")

    spark.sql(
      """
        SELECT id, Name,Salary,City,
         CASE
         when Salary >800 Then "RICH"
         when Salary <800 ANd Salary>400 Then "Middle"
         else "POOR"
         end as Status
         from Details

        """
    ).show()


    //    val window=Window.partitionBy("Name").orderBy("Salary")
    //    df.select(col("id"),col("Name"),col("Salary"),max("Salary").over(window)).show()


  }
}


