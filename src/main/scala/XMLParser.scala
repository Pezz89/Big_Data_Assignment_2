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
 * Format and parse XML data to datasets, ready for further processing using
 * spark
 */
object DataParser {

  /*
   * Generate array of DataFrames from XML content
   */
  def ParseData() : Map[String, DataFrame] = {

    // Define XML file locations and a string of attribute tags to retrieve
    // from each xml element.
    val xmlInfos = Array(
      /*
      ("badges", "../stackoverflow_dataset/badges.txt", "Id UserId Name Date"),
      ("comments", "../stackoverflow_dataset/comments.txt", "Id PostId Score Text CreationDate UserId"),
      ("posts", "../stackoverflow_dataset/posts.txt", "Id PostTypeId ParentID AcceptedAnswerId CreationDate Score ViewCount Body OwnerUserId LastEditorUserId LastEditorDisplayName LastEditDate LastActivityDate CommunityOwnedDate ClosedDate Title Tags AnswerCount CommentCount FavoriteCount"),
      ("postHistory", "../stackoverflow_dataset/postHistory.txt","Id PostHistoryTypeId PostId RevisionGUID CreationDate UserId UserDisplayName Comment Text CloseReasonId"),
      ("postLinks", "../stackoverflow_dataset/postLinks.txt", "Id CreationDate PostId RelatedPostId PostLinkTypeId"),
      */
      ("users", "../stackoverflow_dataset/users.txt", "Reputation CreationDate DisplayName EmailHash LastAccessDate WebsiteUrl Location Age AboutMe Views UpVotes DownVotes", Array[DataType](IntegerType, StringType, StringType, StringType, StringType, StringType, StringType, IntegerType, StringType, IntegerType, IntegerType, IntegerType))
      /*
      ("votes", "../stackoverflow_dataset/votes.txt", "Id PostId VoteTypeId UserId CreationDate")
      */
    )
    
    // Store each file's DataFrame in an array of DataFrames.
    val parsedData = xmlInfos.map(x => (x._1, ParseXMLInfo((x._2, x._3, x._4)))).toMap

    return parsedData
  }

  private def ParseXMLInfo(xmlInfo: (String, String, Array[DataType])) : DataFrame = {
    // Get the XML attributes used for generating the table columns
    var schemaString = xmlInfo._2
    var schemaType = xmlInfo._3
    // Generate schema using XML attribute string
    var schema = GenerateSchemaFromString(schemaString, schemaType)
    // Generate RDD of data from the XML file
    var rdd = ParseInput(xmlInfo._1, schemaString, schemaType)
    // Convert RDD to DataFrame for easier processing
    var data = Main.sqlContext.createDataFrame(rdd, schema)

    return data

  }

  /*
   * Generate a schema based on the string of XML attributes
   */
  private def GenerateSchemaFromString(schemaString: String, schemaType: Array[DataType]) : StructType = {
    var schemaPairs = schemaString.split(" ") zip schemaType
    val fields = schemaPairs.map{case (fieldName: String, dataType: DataType) => StructField(fieldName, dataType, nullable = true)}
    val schema = StructType(fields)
    return schema
  }

  /*
   * Create RDD from XML file
   *
   * inputFilepath: Filepath to XML file
   * schemaString: Space seperated attribute values
   */
  private def ParseInput(inputFilepath: String, schemaString: String, schemaType: Array[DataType]) : RDD[Row] = {
    // Create spark text file object
    val inputFile = Main.sc.textFile(inputFilepath)

    // Map the input file data to an RDD
    val Data = inputFile.map(line => ParsingFunc(line, schemaString, schemaType))
    return Data
  }

  /*
   * Retrieve XML attributes from a String
   *
   * line: XML file line
   * schemaString: Space seperated attribute values
   */
  private def ParsingFunc(line: String, schemaString: String, schemaType: Array[DataType]) : Row = {
    // Parse line of XML using Scala's built in XML library
    val xmlLine = scala.xml.XML.loadString(line)
    var schemaPairs = schemaString.split(" ") zip schemaType
    // Create array of values with element for each attribute in schemaString
    var lineData = schemaPairs.map { case (fieldName: String, dType: DataType) => castToDType(getXMLAttribute(xmlLine, fieldName), dType) }

    return Row.fromSeq(lineData)
  }
  private def castToDType(attribute: String, dType: DataType) : Any = {
    dType match {
      case StringType => return attribute
      case IntegerType =>   try {
                                  return attribute.toInt
                                } catch {
                                  case e: Exception => return -1
                                }
      case DateType => return attribute
    }
  }

  /*
   * Handle NullPointerError raised when an attribute doesn't exist
   *
   * Return an empty string if the attribute doesn't exist
   */
  private def getXMLAttribute(xmlLine: scala.xml.Elem, attribute: String) : String = {
    try { 
      return xmlLine.attributes(attribute).text
    } catch {
      case npe: NullPointerException => return ""
    }
  }
}
