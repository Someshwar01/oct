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

object problems {
  def main(args: Array[String]): Unit = {

    /*
       val sparkconf = new SparkConf()
       sparkconf.set("spark.aap.master", "Spark_Program")
       sparkconf.set("spark.master", "local[*]")

       val spark = SparkSession.builder()
         .config(sparkconf)
         .getOrCreate()


       val spark = SparkSession.builder()
         .appName("Soma")
         .master("local[*]")
         .getOrCreate()

       import spark.implicits._

       val orderData = List(
         ("Order1", "John", 100),
         ("Order2", "Alice", 200),
         ("Order3", "Bobe", 150),
         ("Order4", "Alice", 300),
         ("Order5", "Bob", 250),
         ("Order6", "Johna", 400),
         ("Order7", "soma", 600)
       ).toDF("OrderID", "Customer", "Amount")

       orderData.filter(col("amount") > 300 && col("Customer").endsWith("a")).show()
       orderData.groupBy("customer").agg(count(col("OrderID")), sum(col("Amount"))).show()
   */

    //______________Que Number 2 __ E-Commerce Product Analysis ____________________

    /*

            val spark = SparkSession.builder()
              .appName("Soma")
              .master("local[*]")
              .getOrCreate()

            import spark.implicits._

            val list = List((1, "Laptop", 500),
              (2, "mobile", 850),
              (3, "tab", 600),
              (4, "Laptop", 940),
              (5, "Tv", 840),
              (6, "Video", 912),
              (7, "dvd", 784),
              (8, "Laptop", 846)).toDF("id", "product_name", "price")


            val d1 = list.withColumn("price_category", when(col("Amount") > 500, "Expenaive")
              .when(col("Amount") <= 500 && col("Amount") >= 200, "Moderate").otherwise("Cheap"))
            val d2 = list.filter(col("customer").endsWith("a"))

            d1.show()
            d2.show()
    */


    // _____________________  Problem no.3    _______________________

    /*

                val spark = SparkSession.builder()
                  .appName("Soma")
                  .master("local[*]")
                  .getOrCreate()

                import spark.implicits._

                val orderData = List(
                  ("Order1", "John", 100),
                  ("Order2", "Alice", 200),
                  ("Order3", "Bob", 150),
                  ("Order4", "Alice", 300),
                  ("Order5", "Bob", 250),
                  ("Order6", "John", 400)
                ).toDF("OrderID", "Customer", "Amount")


               orderData.groupBy("customer").agg(count(col("OrderID")),sum(col("Amount"))).show()
    */


    //_____________________  Problem no.4  _________________________

    /*

            val spark = SparkSession.builder()
              .appName("Student_Marks")
              .master("local[*]")
              .getOrCreate()

            import spark.implicits._

            val scoreData = List(
              ("Alice", "Math", 80),
              ("Bob", "Math", 90),
              ("Alice", "Science", 70),
              ("Bob", "Science", 85),
              ("Alice", "English", 75),
              ("Bob", "English", 95)
            ).toDF("Student", "Sub", "Score")
    */


    //============  solved with Scala_Spark  ===========================

    /*
        val d1 = scoreData.groupBy("Sub").agg(avg("Score").as("Avg_Score"))
        val d2 = scoreData.groupBy("Student").agg(max("Score").as("Max_Score"))
        d1.show()
        d2.show()
    */


    // =====================   SOlved by Spark_SQL  ==========================
    /*

            scoreData.createTempView("Details")

            spark.sql(
              """
                SELECT * from scoreData

                """
            ).show()

    */

    //__________________   Problem no.5 (Movie)   _____________________________

    /*

            val spark = SparkSession.builder()
              .appName("Movie")
              .master("local[*]")
              .getOrCreate()

            import spark.implicits._

            val ratingsData = List(
              ("User1", "Movie1", 4.5),
              ("User2", "Movie1", 3.5),
              ("User3", "Movie2", 2.5),
              ("User4", "Movie2", 3.0),
              ("User1", "Movie3", 5.0),
              ("User2", "Movie3", 4.0)
            ).toDF("User", "Movie", "Rating")

            val d1 = ratingsData.groupBy("movie").agg(avg("rating"),sum("rating")).show()
    */


    //__________________  Problem no.6  (avg Temp of city)  _____________________________
    /*

      val sp=SparkSession.builder()
        .appName("City")
        .master("local[*]")
        .getOrCreate()

        import sp.implicits._

        val weatherData = Seq(
          ("City1", "2022-01-01", 10.0),
          ("City1", "2022-01-02", 8.5),
          ("City1", "2022-01-03", 12.3),
          ("City2", "2022-01-01", 15.2),
          ("City2", "2022-01-02", 14.1),
          ("City2", "2022-01-03", 16.8)
        ).toDF("City", "Date", "Temperature")

        weatherData.groupBy("city").agg(min("temperature"),max("temperature"),avg("temperature")).show()
    */

    //__________________   Problem no.7  (Costomer)  _____________________________

    /*

        val spark = SparkSession.builder()
          .appName("Customer")
          .master("local[*]")
          .getOrCreate()

        import spark.implicits._

        val purchaseData = Seq(
          ("Customer1", "Product1", 100),
          ("Customer1", "Product2", 150),
          ("Customer1", "Product3", 200),
          ("Customer2", "Product2", 120),
          ("Customer2", "Product3", 180),
          ("Customer3", "Product1", 80),
          ("Customer3", "Product3", 250)
        ).toDF("Customer", "Product", "Amount")

        purchaseData.groupBy("customer").agg(sum("amount"), count("product")).show()
    */


    //__________________   Problem no.7  (Movie)  _____________________________
    //7. Finding the average rating given by each user for each genre in a movie rating dataset.



    val sp = SparkSession.builder()
      .appName("Movie")
      .master("local[*]")
      .getOrCreate()

    import sp.implicits._

    val ratingData = List(
      ("User1", "Movie1", "Action", 4.5),
      ("User1", "Movie2", "Drama", 3.5),
      ("User1", "Movie3", "Comedy", 2.5),
      ("User2", "Movie1", "Action", 3.0),
      ("User2", "Movie2", "Drama", 4.0),
      ("User2", "Movie3", "Comedy", 5.0),
      ("User3", "Movie1", "Action", 5.0),
      ("User3", "Movie2", "Drama", 4.5),
      ("User3", "Movie3", "Comedy", 3.0)
    ).toDF("User", "Movie", "Genre", "Rating")

    val d1 = ratingData.groupBy("user").agg(avg("rating"))
    val d2 = ratingData.groupBy("genre").agg(count("rating"))
    d1.show()
    d2.show()







  }
}