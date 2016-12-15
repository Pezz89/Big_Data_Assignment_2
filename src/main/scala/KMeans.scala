package ClusterSOData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer

object KMeans {
   /**
    * Run KMeans clustering on an input RDD vector
   */
   //Create a map to store each data row with its closest cluster index as key

  def train(dataset : DataFrame) : RDD[(Int,ArrayBuffer[Float])] = {
    val rows = dataset.rdd
    val K = 5 //number of intended clusters
    val n = rows.count() //number of datapoints
    val m = 3 //number of features
    //var centres = new ArrayBuffer[Row]

    //get random number generator r and use to select K centres randomly from dataset
    /*val r = scala.util.Random
     val random =
    var a = 0
    for (a <- 0 until K) {
      centres(a) = rows(r.ne
    }*/
    val centres = rows.takeSample(false, K, System.nanoTime().toInt)
     val clusterMap :RDD[(Int,Row)]= rows.map(row => (assignCluster(row,centres,m,K),row))
     val newCentres = calculateNewCentres(clusterMap)
     newCentres

  }

  def calculateNorm(datapoint : Row, centre : Row, m: Int): Double = {
    var norm : Double = 0.0
    for (a <- 0 to m) {
      norm = norm + Math.pow(datapoint.getFloat(a) - centre.getFloat(a), 2.0)
    }
    norm = Math.pow(norm, 0.5)
    norm
  }

  def assignCluster(row : Row, centres: Array[Row], m : Int, K :Int): Int = {
    var smallestNorm = 99999999999.0
    var closestCentre = 0
    for (centreNumber <- 0 until K) {
      val norm = calculateNorm(row, centres(centreNumber), m)
      if (norm < smallestNorm) {
        smallestNorm = norm
        closestCentre = centreNumber
      }
    }
    closestCentre
  }

  def calculateNewCentres(clusterMap : RDD[(Int,Row)]): RDD[(Int,ArrayBuffer[Float])] = {
    val data = clusterMap.map(x => (x._1, x._2.asInstanceOf[ArrayBuffer[Float]]))
    val newCentres = data.reduceByKey((a, b) => averageRow(a, b))
    newCentres
  }



    /*for (a <- 0 until K) {
      var cluster = clusterMap.filter{case (a,_) => a == 0}
      var data = cluster.map((_,a) => a :Row)*/



  /*def getCentre(cluster : RDD[(Int,Row)], oldCentre : Row, clusterIndex :Int) : Row = {
    val unWrappedData :RDD[Row] = cluster.map(x => x._2)
    val features : Row = unWrappedData.reduce(averageRow)
    return features
  }*/

  def averageRow(a :ArrayBuffer[Float], b:ArrayBuffer[Float]) : ArrayBuffer[Float] = {
    val newRow = new ArrayBuffer[Float]()
    for (i <- 0 until a.length) {
      val avgI = (a(i) + b(i)) /2
      newRow(i) = avgI
    }
    newRow
  }
}
