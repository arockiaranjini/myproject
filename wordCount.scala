import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wordCount  {
   def main(args:Array[String])
   {

  Logger.getLogger("org").setLevel(Level.ERROR)
  // if any error show me otherwise no need to show


  val sc =new SparkContext("local[*]","wordCount")

  val input = sc.textFile("C:/Ranjini/Ranjini/Dataset/data.txt")
  //input.collect.foreach(println)
  val words =input.flatMap(x => x.split(" "))
  words.collect.foreach(println)
  val wordMap = words.map(x=>(x,1))
  //wordMap.collect.foreach(println)
  val finalCount= wordMap.reduceByKey((x,y) => x+y)
  //finalCount.collect.foreach(println)
 finalCount.saveAsTextFile("C:/Ranjini/Ranjini/Dataset/outnew1")
  //scala.io.StdIn.readLine()
  // this means program is still running not terminated
  //it will show DAG
}
