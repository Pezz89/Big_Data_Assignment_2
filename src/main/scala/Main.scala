package ClusterSOData
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/* 
 * Run KMeans clustering on the StackOverflow dataset
 */
object Main {
  // Initialize spark and SQL to allow for processing of structured data in a
  // spark cluster
  val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans Clustering"))
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val spark = SparkSession.builder.
    master("local")
    .appName("tester")
    .getOrCreate()
  import spark.implicits._

  // Main function for task execution
  def main(args: Array[String]) {
    // Retrieve data from StackOverflow dataset XMLs. Format into DataFrames
    // for easy access to data elements.
    val dataFrames = DataParser.ParseData()
    val a = dataFrames("users")
    a.persist()

    // get the users XML file
    //val users = dataFrames("users")
    //users.persist()
    /*
    // Show 20 entries from the user dataset
    users.show()
    // Show types for the user dataset
    users.printSchema()
    users.show()
    */

    // create new dataframe with only the reputation of the users
    //val a = users.select("Reputation").rdd.map(r => r(0)).persist()
    a.take(2).foreach(f => println(f(1)))
    
    

    // Info on using DataFrames here: https://www.mapr.com/blog/using-apache-spark-dataframes-processing-tabular-data
  }
}

