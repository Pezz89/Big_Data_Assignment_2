package ClusterOSData
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Main {
  val sc = new SparkContext(new SparkConf().setAppName("Spark KMeans Clustering"))
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]) {
    KMeans.run()
    DataParser.ParseData()
  }

  object DataParser {

    // case class BadgeRow(i: String, j: String, k: String, m: String)
    // case class VotesRow(i: String, j: String, k: String, m: String, m: String)
    // case class CommentsRow(i: String, j: String, k: String, m: String)
    //case class BadgeRow(i: String, j: String, k: String, m: String)
    //case class BadgeRow(i: String, j: String, k: String, m: String)

    def ParseData() {
      // The schema is encoded in a string
      var schemaString = "Id UserId Name Date"

      // Generate the schema based on the string of schema
      var fields = schemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      var schema = StructType(fields)
      var rdd = ParseInput("../stackoverflow_dataset/badges.txt", schemaString)
      val badgeData = sqlContext.createDataFrame(rdd, schema)
      badgeData.show()
      
      // The schema is encoded in a string
      schemaString = "Id PostId VoteTypeId UserId CreationDate"

      // Generate the schema based on the string of schema
      fields = schemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      schema = StructType(fields)
      rdd = ParseInput("../stackoverflow_dataset/votes.txt", schemaString)
      val voteData = sqlContext.createDataFrame(rdd, schema)
      voteData.show()
      // ParseInput("../stackoverflow_dataset/comments.txt")
      // ParseInput("../stackoverflow_dataset/postHistory.txt")
      // ParseInput("../stackoverflow_dataset/postLinks.txt")
    }

    private def ParseInput(inputFilepath: String, schemaString: String) : RDD[Row] = {
      val inputFile = Main.sc.textFile(inputFilepath)

      val Data = inputFile.map(line => ParsingFunc(line, schemaString))
      return Data
    }

    private def ParsingFunc(line: String, schemaString: String) : Row = {
      val xmlLine = scala.xml.XML.loadString(line)
      var lineData = schemaString.split(" ").map(fieldName => getXMLAttribute(xmlLine, fieldName))

      println(lineData)
      return Row.fromSeq(lineData)
    }

    private def getXMLAttribute(xmlLine: scala.xml.Elem, attribute: String) : String = {
      try { 
        return xmlLine.attributes(attribute).text
      } catch {
        case npe: NullPointerException => return ""
      }
    }
  }
}


