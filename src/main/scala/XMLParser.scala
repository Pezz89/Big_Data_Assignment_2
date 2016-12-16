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
  def ParseData() : Map[String, RDD[Row]] = {

    // Define XML file locations and a string of attribute tags to retrieve
    // from each xml element.
    val xmlInfos = Array(
      /*
      ("badges", "/data/stackoverflow/Badges", "Id UserId Name Date", Array[DataType](IntegerType, IntegerType, StringType, DateType)),
      ("comments", "/data/stackoverflow/Comments", "Id PostId Score Text CreationDate UserId", Array[DataType](IntegerType, IntegerType, IntegerType, StringType, DateType, IntegerType)),
      ("posts", "data/stackoverflow/Posts", "Id PostTypeId ParentID AcceptedAnswerId CreationDate Score ViewCount Body OwnerUserId LastEditorUserId LastEditorDisplayName LastEditDate LastActivityDate CommunityOwnedDate ClosedDate Title Tags AnswerCount CommentCount FavoriteCount", Array[DataType](IntegerType, IntegerType, IntegerType, IntegerType, DateType, IntegerType, IntegerType, StringType, IntegerType, IntegerType, StringType, DateType, DateType, DateType, DateType, StringType, StringType, IntegerType, IntegerType, IntegerType)),
      ("postHistory", "/data/stackoverflow/PostHistory","Id PostHistoryTypeId PostId RevisionGUID CreationDate UserId UserDisplayName Comment Text CloseReasonId", Array[DataType](IntegerType, IntegerType, IntegerType,IntegerType, DateType, IntegerType, StringType, StringType, StringType, IntegerType)),
      ("postLinks", "data/stackoverflow/PostLinks", "Id CreationDate PostId RelatedPostId PostLinkTypeId", Array[DataType](IntegerType, DateType, IntegerType, IntegerType, IntegerType)),
      */
      ("users", "/data/stackoverflow/Users", "Reputation CreationDate DisplayName EmailHash LastAccessDate WebsiteUrl Location Age AboutMe Views UpVotes DownVotes", Array[DataType](IntegerType, DateType, StringType, StringType, DateType, StringType, StringType, IntegerType, StringType, IntegerType, IntegerType, IntegerType))
      /*
      ("votes", "/data/stackoverflow/Votes", "Id PostId VoteTypeId UserId CreationDate", Array[DataType](IntegerType, IntegerType, IntegerType, IntegerType, DateType))
      */
     )
    
    // Store each file's DataFrame in an array of DataFrames.
    val parsedData = xmlInfos.map(x => (x._1, ParseXMLInfo((x._2, x._3, x._4)))).toMap

    return parsedData
  }

  private def ParseXMLInfo(xmlInfo: (String, String, Array[DataType])) : RDD[Row] = {
    // Get the XML attributes used for generating the table columns
    var schemaString = xmlInfo._2
    val schemaType = xmlInfo._3
    // Generate schema using XML attribute string
    var schema = GenerateSchemaFromString(schemaString, schemaType)
    // Generate RDD of data from the XML file
    var rdd = ParseInput(xmlInfo._1, schemaString, schemaType)
    // Convert RDD to DataFrame for easier processing
    //var data = Main.sqlContext.createDataFrame(rdd, schema)

    return rdd

  }

  /*
   * Generate a schema based on the string of XML attributes
   */
  private def GenerateSchemaFromString(schemaString: String, schemaType: Array[DataType]) : StructType = {
    // Replace all DateTypes with Longs as date will now be stored as longs.
    val sT = schemaType.map(i => if (i==DateType) LongType else i)
    val schemaPairs = schemaString.split(" ") zip sT

    // Create schema for columns and set their datatypes for DataFrame based on attribute names.
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
    val Data = inputFile.collect {
      case line if !SantizeLine(line) => ParsingFunc(line, schemaString, schemaType)
    }
    return Data
  }

  /*
   * Retrieve XML attributes from a String
   *
   * line: XML file line
   * schemaString: Space seperated attribute values
   */

  private def SantizeLine(line: String) : Boolean = {
    val invalidLines = Array("<?xml version=\"1.0\" encoding=\"utf-8\"?>", "<users>", "</users>")
    return invalidLines contains line
  }

  private def ParsingFunc(line: String, schemaString: String, schemaType: Array[DataType]) : Row = {
    // Parse line of XML using Scala's built in XML library
    val xmlLine = scala.xml.XML.loadString(line)
    var schemaPairs = schemaString.split(" ") zip schemaType
    // Create array of values with element for each attribute in schemaString
    var lineData = schemaPairs.map { case (fieldName: String, dType: DataType) => castToDType(getXMLAttribute(xmlLine, fieldName), dType) }

    return Row.fromSeq(lineData)
  }

  /*
   * Cast attribute data to relevant datatype.
   */
  private def castToDType(attribute: String, dType: DataType) : Any = {
    dType match {
      case StringType => return attribute
      case IntegerType =>   
        try {
          return attribute.toInt
        } catch {
          // If the string was not castable to integer then it is not a number.
          // In this case, return a placeholder value of -1.
          case e: Exception => return -1
        }
      case DateType => 
        // If the string is a date, convert from date string to long.
        var format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        return format.parse(attribute).getTime() 
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
