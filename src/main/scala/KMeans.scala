import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._

object KMeans {
   /* This is my first java program.  
   * This will print 'Hello World' as the output
   */
    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans Clustering"))
      val inputfile = sc.textFile("../stackoverflow_dataset/badges.txt")
      val counts = inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_);
      counts.saveAsTextFile("output")
   }
}
