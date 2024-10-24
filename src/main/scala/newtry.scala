import org.apache.spark.SparkContext

object newtry {
  def main(args:Array[String]):Unit= {



 val sc=new SparkContext("local[2]","Someshwar")
 val input=sc.textFile("C:/Users/anike/Desktop/check.txt")
    val rdd1=input.flatMap(x=>x.split(" "))
    val rdd2=rdd1.map(x=>(x,1))
    val rdd3=rdd2.reduceByKey((x,y)=>x+y)
    val rdd4=rdd3.sortBy(x=>x._2,false)
    rdd4.take(6).foreach(println)

    scala.io.StdIn.readLine()




  }


}


