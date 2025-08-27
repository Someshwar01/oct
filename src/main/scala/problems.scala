import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object problems {
  def main(args: Array[String]): Unit = {
    //    val logger: Logger = Logger.getLogger(this.getClass)

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


    /*

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
    */


    // -------------------------------------------------------------------------------------------------------------------------------
    //    If salary is less than previous month we will mark it as DOWN, if salary has increased then UP


    //        val spark = SparkSession.builder()
    //          .appName("SekhoBD")
    //          .master("local[*]")
    //          .getOrCreate()
    //
    //    import spark.implicits._
    //    val salaryData = Seq(
    //      (1, "John", 1000, "01/01/2016"),
    //      (1, "John", 2000, "02/01/2016"),
    //      (1, "John", 1000, "03/01/2016"),
    //      (1, "John", 2000, "04/01/2016"),
    //      (1, "John", 3000, "05/01/2016"),
    //      (1, "John", 1000, "06/01/2016")).toDF("ID", "NAME", "SALARY", "DATE")
    //
    //    val TO_Date = salaryData.withColumn("DATE", to_date(col("DATE"), "MM/dd/yyyy"))
    //    val window = Window.orderBy("DATE")
    //    val df = TO_Date.select(
    //      col("*"), lag(col("SALARY"), 1).over(window).as("previous_Salary"))
    //    val df2 = df.withColumn("status", when(col("SALARY") > col("previous_Salary"), lit("UP")).otherwise(lit("Down")))
    //        df2.show()
    //    TO_Date.show()


    //_____________________________  New Problem  ________________________________________________________

    //    1. we want to find the difference between
    //    the price on each day with it’s previous day.

    //    import spark.implicits._

    //    val saleData = Seq(
    //      (1, "KitKat", 1000.0, "2021-01-01"),
    //      (1, "KitKat", 2000.0, "2021-01-02"),
    //      (1, "KitKat", 1000.0, "2021-01-03"),
    //      (1, "KitKat", 2000.0, "2021-01-04"),
    //      (1, "KitKat", 3000.0, "2021-01-05"),
    //      (1, "KitKat", 1000.0, "2021-01-06")
    //    ).toDF("IT_ID", "IT_Name", "Price", "PriceDate")
    //
    //    val window = Window.partitionBy("IT_ID").orderBy("PriceDate")
    //    val previousPrice = saleData.withColumn("previousPrice", lag("Price", 1).over(window))
    //    previousPrice.show()
    //
    //    val priceDifference = previousPrice.withColumn("priceDifference", $"Price" - $"previousPrice")
    //    priceDifference.show()

    //_______________________________________--------------NEXT PROBLEM -------------_________________________________________

    val spark = SparkSession.builder()
      .appName("SekhoBD")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val sampleData = Seq(
      (1, "karthik", 1000),
      (2, "mohan", 2000),
      (3, "vinay", 1500),
      (4, "deva", 3000)
    ).toDF("id", "name", "salary")

    //    ___________________________------------Q3------------____________________________
    //    val windows = Window.orderBy("id")
    //    val leadLagData = sampleData.withColumn("previous_salary", lag("salary", 1).over(windows))
    //      .withColumn("next_salary", lead("salary", 1).over(windows))
    //    leadLagData.show()

    //    _________________________---------------Q4----------_____________________________
    //    val percentageDifferenceDF = leadLagData.withColumn("percentage_difference",
    //      ($"salary" - $"previous_salary") / $"previous_salary" * 100)
    //    percentageDifferenceDF.show()

    //    _________________________---------------Q5----------_____________________________
    //    calculate the rolling sum of salary
    //    for current row and the pervious two rows
    //    , ordered by id

    //    val windowSpec = Window.orderBy("id").rowsBetween(-2, 0)
    //    val rollingSumData = sampleData.withColumn("rolling_sum", sum("salary").over(windowSpec))
    //    rollingSumData.show()

    //    _________________________---------------Q6----------_____________________________

    //
    //    // Define the window specification
    //     Calculate the minimum salary within the last 3 rows
    //    val dataWithMinSalary = sampleData.withColumn("min_salary", min("salary").over(windowSpec))

    //    // Calculate the difference between current salary and the minimum salary
    //    val dataWithSalaryDiff = dataWithMinSalary.withColumn("salary_diff", $"salary" - $"min_salary")
    //    dataWithSalaryDiff.show()


    //--------------------------------OR------------------------
    //    val minSalary = sampleData.withColumn("minSalary", min("salary").over(windowSpec))
    //    val salDifference = minSalary.withColumn("salaryDifference", $"salary" - $"minSalary")
    //        salDifference.show()

    //    _________________________---------------Q7----------_____________________________


    //    val windowpart = Window.partitionBy("name").orderBy("id")
    //    val employeeGroup = sampleData.withColumn("lead_salary", lead("salary", 1).over(windowpart))
    //      .withColumn("lag_salary", lag("salary", 1).over(windowpart))
    //        employeeGroup.show()


    //    _________________________---------------Q8----------_____________________________

    //    val salaryGreaterThan1500 = leadLagData.withColumn("leadGreaterThen1500", lead("salary", 1).over(windowpart))
    //      .withColumn("lagGreaterThan1500", lag("salary", 1).over(windowpart))
    //    val df = salaryGreaterThan1500.filter(col("salary") > 1500)
    //        df.show()

    //    _________________________---------      NOT SOLVED    ------Q9----------_____________________________

    //    val changeSalary = leadLagData.withColumn("changeInSalaryLead", lead("salary", 1).over(windowpart))
    //      .withColumn("changeInSalaryLag", lag("salary", 1).over(windowpart))
    //    val changeInSalary = changeSalary.filter(abs(col("salary") - col("changeInSalaryLag")) > 500)
    //    changeInSalary.show()

    //    _________________________---------------Q_10----------_____________________________

    //    val cumulativeCount=leadLagData.withColumn("cumulativeCount",count("id").over(windowpart))
    //    cumulativeCount.show()
    //
    //    _________________________---------------Q_11----------_____________________________

    //    val wid = Window.partitionBy("name").orderBy(asc("id")).rowsBetween(-1, 0)
    //    val runningSalary = leadLagData.withColumn("runningSalary", sum("salary").over(wid))
    //    runningSalary.show()

    //    _________________________---------------Q_12----------_____________________________

    //    val maxSalary=leadLagData.withColumn("maxSalary",max("salary").over(windowpart))
    //    maxSalary.show()

    //    _________________________---------------Q_13----------_____________________________

    //    val avgSalary = leadLagData.withColumn("avgSalary", avg("salary").over(windowpart))
    //    val avgSalaryDifference = avgSalary.withColumn("avgSalaryDifference", col("salary") - col("avgSalary"))
    //    avgSalaryDifference.show()

    //    _________________________---------------Q_14----------_____________________________
    //    val windowrank = Window.partitionBy("name").orderBy(desc("salary"))
    //    val employeeRank = sampleData.withColumn("rank", rank().over(windowrank))
    //    employeeRank.show()


    //    _________________________---------------Q_15----------_____________________________

    //    val windowptor = Window.orderBy("id")
    //    val leadLagSal = sampleData.withColumn("lag_salary", lag("salary", 1).over(windowptor))
    //      .withColumn("lead_salary", lead("salary", 1).over(windowptor))
    //    val increasedSalary = leadLagSal.withColumn("increasedSalary", (col("salary") - col("lag_salary")) > 1).filter(col("increasedSalary") === "true")
    //    increasedSalary.show()

    //============================Q_15===============================================

    import spark.implicits._
    val Data = Seq(
      (1, "karthik", 1000),
      (4, "deva", 7000),
      (3, "vinay", 6500),
      (2, "mohan", 2000),
      (4, "deva", 3900),
      (1, "karthik", 6600),
      (3, "vinay", 1500),
      (1, "karthik", 2500),
      (4, "deva", 1300)
    ).toDF("id", "name", "salary")

    val window = Window.orderBy(asc("id"))
    //    val df=sampleData.withColumn("prev_Emp_Salary",lag(col("salary"),1).over(window))
    //    val df2=df.filter(col("salary")>col("prev_Emp_Salary"))
    //    df2.show()

    //============================Q_16===============================================

    //    val window = Window.partitionBy("name").orderBy("id")
    //    val df = Data.withColumn(" past_month_salary", lag("salary", 1).over(window))
    //    df.show()


    //============================Q_17===============================================

    val old_sal = Data.withColumn("previous_sal", lag(col("salary"), 1).over(window))
    val changes = old_sal.withColumn("per_salary", when(col("per_change").isNotNull, ((col("previous_sal") - col("salary")) / col("salary")) * 100))
    changes.show()


  }
}

//    val employees =


//    val windowSepc = Window.o


//    val spark = SparkSession.builder
//      .master("local[*]")
//      .appName("ReadArgsExample").getOrCreate()
//
//    // Read arguments from the command line
//    val inputPath = args(0) // First argument
//    val outputPath = args(1) // Second argument
//
//    // Read data from the input path
//    val df = spark.read.option("header", "true").csv("C:/Users/anike/Desktop/spark")
//
//    // Show the DataFrame
//    df.show()
//
//    // Write the DataFrame to the output path
//    df.write.mode("overwrite").option("header", "true").csv(outputPath)
//
//    spark.stop()


