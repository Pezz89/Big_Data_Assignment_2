package bdp.spark.KMeans

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkKMeans {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans"))
    
    val lines = sc.textFile(args(0))
    val featureSet = lines.map(getFeatures)

    featureSet.foreach(println)
     
  }

  def getFeatures(line :String) : [String] = {

  	val featureIDs = [" rowID="," Reputation="," CreationDate="," LastAccessDate="," Views="," UpVotes="," DownVotes="," Age="]
    val fragments = line.split("\"")
    var features = []

    for (a <- 0 to 7) {
    	if (fragments.contains(featureIDs(a))) {
    		val index = fragments.indexOf(featureIDs(a))
            features(a) = fragments(index + 1)
        } else {
        	features(a) = ""
        }
    }
    return features
  }
}
