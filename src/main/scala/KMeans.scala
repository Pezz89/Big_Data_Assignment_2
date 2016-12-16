package ClusterSOData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

object KMeans {
   /**
    * Run KMeans clustering on an input RDD vector
   */
   //Create a map to store each data row with its closest cluster index as key
   var centres : ArrayBuffer[Float] = ArrayBuffer(0.0f, 100.0f)

  def train(dataset : DataFrame, iterations:Int) : Unit = {
     val relevantData = dataset.select("Reputation")
    val rows = relevantData.rdd
    //val rowsAsArray = rows.map(row => List(row.getInt(0).toFloat, row.getInt(1).toFloat, row.getInt(2).toFloat) )
    val rowsAsArray = rows.map(row => row.getInt(0).toFloat )
    //rowsAsArray.foreach(println)
    val K = 2 //number of intended clusters
    //val n = rows.count() //number of datapoints
    val m = 1 //number of features
    //var centres = new ArrayBuffer[Row]

    for (i <- 0 to iterations) {
      val clusterMap :RDD[(Int,Float)]= rowsAsArray.map(row => (assignCluster(row,centres,m,K),row))
      val newCentres = clusterMap.reduceByKey((a,b) => getAverage(a,b))
      println("Average reputation is:")
      val results = newCentres.map(x => x._2)
      val resultsOutput = results.collect()
      for (i <- 0 until K) {
        centres(i) = resultsOutput(i)
      }
    }

    //get random number generator r and use to select K centres randomly from dataset
    /*val r = scala.util.Random
     val random =
    var a = 0
    for (a <- 0 until K) {
      centres(a) = rows(r.ne
    }*/
    //val centres = rowsAsArray.takeSample(false, K, System.nanoTime().toInt)
     //val centres : Array[List[Float]] = Array(List(0.0f, 0.0f, 0.0f), List(10.0f, 10.0f, 10.0f), List(20.0f, 20.0f, 20.0f))
     
     //val centre = 0.0f

     
     //val newCentres = calculateNewCentres(clusterMap)
     //val newCentre = rowsAsArray.reduce((a,b) => getAverage(a,b))



  }
/*
  def calculateNorm(datapoint : List[Float], centre : List[Float], m: Int): Double = {
    var norm : Double = 0.0
    for (a <- 0 until m) {
      norm = norm + Math.pow(datapoint(a) - centre(a), 2.0)
    }
    norm = Math.pow(norm, 0.5)
    norm
  }*/

  def assignCluster(row : Float, centres: ArrayBuffer[Float], m : Int, K :Int): Int = {
    var smallestNorm = 999999.0
    var closestCentre = 0
    for (centreNumber <- 0 until K) {
      val norm = Math.abs(row - centres(centreNumber))
      //val norm = calculateNorm(row, centres(centreNumber), m)
      if (norm < smallestNorm) {
        smallestNorm = norm
        closestCentre = centreNumber
      }
    }
    closestCentre
  }

/*

  def calculateNewCentres(clusterMap : RDD[(Int,List[Float])]): RDD[(Int,List[Float])] = {
    //val data = clusterMap.map(x => (x._1, x._2.asInstanceOf[ArrayBuffer[Double]]))
    val newCentres = clusterMap.reduceByKey((a, b) => averageRow(a, b))
    //val singleCluster = clusterMap.filter(x => x._1 == 0)
    //val singleClusterAsArray = singleCluster.reduce()
    newCentres
  }*/




    /*for (a <- 0 until K) {
      var cluster = clusterMap.filter{case (a,_) => a == 0}
      var data = cluster.map((_,a) => a :Row)*/



  /*def getCentre(cluster : RDD[(Int,Row)], oldCentre : Row, clusterIndex :Int) : Row = {
    val unWrappedData :RDD[Row] = cluster.map(x => x._2)
    val features : Row = unWrappedData.reduce(averageRow)
    return features
  }*/

  /*def averageRow(a :ArrayBuffer[Float], b:ArrayBuffer[Float]) : ArrayBuffer[Float] = {
    val newRow = Row.apply()
    for (i <- a.indices) {
      val avgI = (a(i) + b(i)) /2
      newRow(i) = avgI
    }
    newRow
  }*/

  def averageRow(a:List[Float], b:List[Float]) : List[Float] = {
    val means = new ArrayBuffer[Float]
    for (i <- 0 until a.size) {
      val mean = (a(i) + b(i)) /2.0f
      means(i) = mean
    }
    return means.toList
  }

  def getAverage(a: Float, b:Float) : Float = {
    return ((a+b)/2)
  }

}
