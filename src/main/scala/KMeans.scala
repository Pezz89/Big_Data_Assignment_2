package ClusterSOData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


object KMeans {
  /**
    * Run KMeans clustering on an input RDD vector
    */


  def train(dataset : DataFrame, iterations:Int) : Unit = {
    val K = 10 // Number of desired clusters
    val relevantData = dataset.select("Reputation", "Views", "UpVotes", "DownVotes")
    val m = relevantData.columns.length  //number of features
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

    var counts = Array[Int](K)

    for (i <- 0 until iterations) {
      val clusterMap = clustering(centres, rowsAsArray, m, K).persist()
      centres = getCentres(clusterMap, m, K)
      counts = getCounts(clusterMap, K)
      //At last iteration, save clustering results to file on hadoop fs
      if (i == iterations -1) {
        val printableResults = clusterMap.map(x => (x._1, x._2.mkString(",")))
        printableResults.saveAsTextFile("spark_output/results.txt")
      }
      clusterMap.unpersist()
      if (centres == null || counts == null) {
        println("Error, starting again")
        train(dataset, iterations)
        return
      }
      println("\niteration " + i + " :")
      for (j <- 0 until K) {
        println("centre " + j + " = " + centres(j).mkString("[",",","]") + " count = " + counts(j) )
      }
    }
  }


  def clustering(centres :Array[Array[Float]], rowsAsArray : RDD[Array[Float]], m : Int, K : Int) : RDD[(Int,Array[Float])] = {
    val clusterMap :RDD[(Int,Array[Float])]= rowsAsArray.map(row => (assignCluster(row,centres,m,K),row))
    return clusterMap

  }

  def getCentres(clusterMap: RDD[(Int,Array[Float])], m: Int, K: Int) : Array[Array[Float]] = {
    val newCentres = clusterMap.reduceByKey((a,b) => getMeanVector(a,b,m))
    val arrayNewCentres = newCentres.collect()

    var results = new Array[Array[Float]](K)
    for ((i,x) <- arrayNewCentres) {
      results(i) = x
    }
    //Check all results are valid (no null)
    for (i <- 0 until K) {
      if (results(i) == null) {
        return null
      }
    }
    return results
  }

  def getCounts(clusterMap: RDD[(Int,Array[Float])], K: Int) : Array[Int] = {
    val countable = clusterMap.map(x => (x._1, 1))
    val countsWithKeys = countable.reduceByKey((a, b) => a + b).collect()
    var counts = new Array[Int](K)
    for ((i, x) <- countsWithKeys) {
      counts(i) = x
    }
    for (i <- 0 until K) {
      if (counts(i) == 0) {
        return null
      }
    }
    return counts
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
