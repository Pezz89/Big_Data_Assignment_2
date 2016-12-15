package ClusterSOData

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KMeans {
   /**
    * Run KMeans clustering on an input RDD vector
   */
   //Create a map to store each data row with its closest cluster index as key
   var clusterMap : mutable.HashMap[Int,Row]

  def train(dataset : DataFrame) {
    val rows = dataset.collect()
    val K = 5 //number of intended clusters
    val n = rows.length //number of datapoints
    val m = 3 //number of features
    var centres = new ArrayBuffer[Row]

    //get random number generator r and use to select K centres randomly from dataset
    val r = scala.util.Random
    var a = 0
    for (a <- 0 until K) {
      centres(a) = rows(r.nextInt(n))
    }
    assignClusters(rows,centres, m)
  }

  def calculateNorm(datapoint : Row, centre : Row, m: Int): Double = {
    var norm : Double = 0.0
    for (a <- 0 to m) {
      norm = norm + Math.pow(datapoint.getFloat(a) - centre.getFloat(a), 2.0)
    }
    norm = Math.pow(norm, 0.5)
  }

  def assignClusters(rows : Array[Row], centres: ArrayBuffer[Row], m : Int): Unit = {
    for (row <- rows) {
      var greatestNorm = 0.0
      var closestCentre = 0
      for (centreIndex <- 0 until centres.length) {
        val norm = calculateNorm(row, centres(centreIndex), m)
        if (norm > greatestNorm) {
          greatestNorm = norm
          closestCentre = centreIndex
        }
      }
      clusterMap.put(closestCentre, row)
    }
  }
}
