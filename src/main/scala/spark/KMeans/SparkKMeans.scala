package bdp.spark.KMeans

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkKMeans {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans"))
    
    val lines = sc.textFile(args(0))

    lines.foreach(println)
    //val lines = sc.textFile("hdfs://moonshot-ha-nameservice" + args(0))

    //val ages = lines.map(getAge)


    //ages.foreach(println)



    //transformations from the original input data
    //val words = lines.flatMap(line => line.split("[ .,;:()]+"))
    //val pairs = words.map(word => (word,1))
    //val counts = pairs.reduceByKey((a,b)=>a+b)

    //collect() is an action, so this value is retrieved to the driver
    //val winter = counts.filter(pair=>pair._1.equals("winter")).collect()    
    
    //winter.foreach(println)
     
  }

  def getAge(line :String) : String = {
    //return ((line.split("Age="))(1)).substring(1,3)
    return (line.split("Age="))(1)
  }
}
