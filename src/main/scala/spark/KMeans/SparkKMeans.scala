package bdp.spark.KMeans

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkKMeans {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans"))
    
    val lines = sc.textFile(args(0))
    val ages = lines.map(getAge)

    ages.foreach(println)
     
  }

  def getAge(line :String) : String = {
    val fragments = line.split("\"")
    var age = "0"
    if (fragments.contains(" Age=")) {
      val index = fragments.indexOf(" Age=")
      age = fragments(index + 1)
    }
    return age
  }
}
