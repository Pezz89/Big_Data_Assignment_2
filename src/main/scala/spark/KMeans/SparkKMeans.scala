package bdp.spark.KMeans

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkKMeans {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans"))
    val lines = sc.textFile(args(0))
    val featureSet = lines.map(getFeatures)
    val printableFeatureSet = featureSet.map(makeListPrintable)
    printableFeatureSet.foreach{println}
     
  }

  def getFeatures(line :String) : List[String] = {
    
    val fragments = line.split("\"")
   
    val featureIDs = List(" Reputation="," CreationDate="," LastAccessDate="," Views="," UpVotes="," DownVotes="," Age=")
    
    var features = new ListBuffer[String]()
    features += fragments(1)
    
    var a = ""
    for (a <- featureIDs) {
    	if (fragments.contains(a)) {
    	    val index = fragments.indexOf(a)
            features += fragments(index + 1)
        } else {
            features += ""
        }
    }

    features(2) = features(2).substring(0,10)
    features(3) = features(3).substring(0,10)
    
    val featuresList = features.toList
    return featuresList
  }

  def makeListPrintable(featureList : List[String]) : String = {
        return featureList.mkString(", ")
  }
}
