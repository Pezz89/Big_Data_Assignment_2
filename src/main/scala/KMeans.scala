package ClusterSOData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

object KMeans {
  /**
    * Run KMeans clustering on an input RDD vector
    */


  def train(dataset : DataFrame, iterations:Int) : Unit = {
    val K = 10
    val m = 4
    val relevantData = dataset.select("Reputation", "Views", "UpVotes", "DownVotes")
    val rows = relevantData.rdd
    val rowsAsArray = rows.map(row => convertRow(row, m)).persist()


    var centres: Array[Array[Float]] = rowsAsArray.takeSample(false, K, System.nanoTime().toInt)
    //To reduce chance of two random centres being the same, add a changing value to each
    println("centres initialised")
    for (i <- 0 until K) {
      for (j <- 0 until m) {
        centres(i)(j) += i+j
      }
      println("centre " + i + " = " + centres(i).mkString("[",",","]") )
    }


    for (i <- 0 until iterations) {
      centres = clustering(centres, rowsAsArray, m, K)
      println("\niteration " + i + " :")
      for (j <- 0 until K) {
        println("centre " + j + " = " + centres(j).mkString("[",",","]") )
      }
    }
  }


  def clustering(centres :Array[Array[Float]], rowsAsArray : RDD[Array[Float]], m : Int, K : Int) : Array[Array[Float]] = {
    val clusterMap :RDD[(Int,Array[Float])]= rowsAsArray.map(row => (assignCluster(row,centres,m,K),row))
    val newCentres = clusterMap.reduceByKey((a,b) => getMeanVector(a,b,m))
    val arrayNewCentres = newCentres.collect()

    var results = new Array[Array[Float]](K)
    for ((i,x) <- arrayNewCentres) {
      results(i) = x
    }
    results
  }



    def calculateNorm(datapoint : Array[Float], centre : Array[Float], m: Int): Double = {
      var norm : Double = 0.0
      for (a <- 0 until m) {
        norm = norm + Math.pow(datapoint(a) - centre(a), 2.0)
      }
      norm = Math.pow(norm, 0.5)
      norm
    }

  def assignCluster(row : Array[Float], centres: Array[Array[Float]], m : Int, K :Int): Int = {
    var smallestNorm = 999999.0
    var closestCentre = 0
    for (centreNumber <- 0 until K) {
      //val norm = Math.abs(row - centres(centreNumber))
      val norm = calculateNorm(row, centres(centreNumber), m)
      if (norm < smallestNorm) {
        smallestNorm = norm
        closestCentre = centreNumber
      }
    }
    closestCentre
  }

  def averageRow(a:List[Float], b:List[Float]) : List[Float] = {
    val means = new ArrayBuffer[Float]
    for (i <- 0 until a.size) {
      val mean = (a(i) + b(i)) /2.0f
      means(i) = mean
    }
    return means.toList
  }


  def getMeanVector(a: Array[Float], b: Array[Float], m: Int) : Array[Float] = {
    var means = new Array[Float](m)
    for (i <- 0 until m) {
      means(i) = (a(i) + b(i)) / 2
    }
    means
  }

  def convertRow(row : Row, m: Int) : Array[Float] = {
    var dataArray = new Array[Float](m)
    for (i <- 0 until m) {
      dataArray(i) = row.getInt(i).toFloat
    }
    dataArray
  }

}
