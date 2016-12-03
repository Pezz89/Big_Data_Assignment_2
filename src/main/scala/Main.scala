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
    val df = DataParser.ParseData(sc, sqlContext)
  }

  object DataParser {
    def ParseData(sparkc: SparkContext, sqlc: SQLContext) : Array[DataFrame] = {

      val xmlInfos = Array(
        ("../stackoverflow_dataset/badges.txt", "Id UserId Name Date"),
        ("../stackoverflow_dataset/comments.txt", "Id PostId Score Text CreationDate UserId"),
        ("../stackoverflow_dataset/posts.txt", "Id PostTypeId ParentID AcceptedAnswerId CreationDate Score ViewCount Body OwnerUserId LastEditorUserId LastEditorDisplayName LastEditDate LastActivityDate CommunityOwnedDate ClosedDate Title Tags AnswerCount CommentCount FavoriteCount"),
        ("../stackoverflow_dataset/postHistory.txt","Id PostHistoryTypeId PostId RevisionGUID CreationDate UserId UserDisplayName Comment Text CloseReasonId"),
        ("../stackoverflow_dataset/postLinks.txt", "Id CreationDate PostId RelatedPostId PostLinkTypeId"),
        ("../stackoverflow_dataset/users.txt", "Reputation CreationDate DisplayName EmailHash LastAccessDate WebsiteUrl Location Age AboutMe Views UpVotes DownVotes"),
        ("../stackoverflow_dataset/votes.txt", "Id PostId VoteTypeId UserId CreationDate")
      )
      
      
      val parsedData = xmlInfos.map(x => ParseXMLInfo(x))

      for(i <- 0 until parsedData.length){
        parsedData(i).show()
      }
      return parsedData
    }

    private def ParseXMLInfo(xmlInfo: (String, String)) : DataFrame = {
      // The schema is encoded in a string
      var schemaString = xmlInfo._2
      var schema = GenerateSchemaFromString(schemaString)
      var rdd = ParseInput(xmlInfo._1, schemaString)
      var data = sqlContext.createDataFrame(rdd, schema)
      return data

    }

    private def GenerateSchemaFromString(schemaString: String) : StructType = {
      // Generate the schema based on the string of schema
      val fields = schemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val schema = StructType(fields)
      return schema
    }

    private def ParseInput(inputFilepath: String, schemaString: String) : RDD[Row] = {
      val inputFile = Main.sc.textFile(inputFilepath)

      val Data = inputFile.map(line => ParsingFunc(line, schemaString))
      return Data
    }

    private def ParsingFunc(line: String, schemaString: String) : Row = {
      val xmlLine = scala.xml.XML.loadString(line)
      var lineData = schemaString.split(" ").map(fieldName => getXMLAttribute(xmlLine, fieldName))

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



