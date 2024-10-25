import org.apache.spark.SparkContext


object newtry {
  def main(args: Array[String]): Unit = {


    //
    // val sc=new SparkContext("local[2]","Someshwar")
    // val input=sc.textFile("C:/Users/anike/Desktop/check.txt")
    //    val rdd1=input.flatMap(x=>x.split(" "))
    //    val rdd2=rdd1.map(x=>(x,1))
    //    val rdd3=rdd2.reduceByKey((x,y)=>x+y)
    //    val rdd4=rdd3.sortBy(x=>x._2,false)
    //    rdd4.take(2).foreach(println)
    //
    //    scala.io.StdIn.readLine()
    //


    //    for (i <- 0 to 11) {
    //      for (j <- 0 to 11) {
    //
    //        if (i == 10 || j == 10 || i + j == 10) {
    //
    //          println("Result " + i, j + " : True")
    //        }
    //      }
    //    }


    //    val tup =(10,"scala",true,45,10,12,10)
    //    tup.productIterator.foreach(println)
    val set = Set(10, 20, 3, 5, 80, 40, 50)

    for (el <- set) {

      println(el)
    }

    //    val es =set.contains(3)
    //    print(es)
    //  print(tup._4)

    var op = 55
    while (op <= 67) {
      print(op +"  ")
      op+=1
    }


  }
}






