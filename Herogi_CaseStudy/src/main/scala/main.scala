import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.util.{Try, Success, Failure}
import scala.math.BigDecimal

object TestApp extends App {
  lazy implicit val spark = SparkSession.builder().master("local").appName("spark_test").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._ // Required to call the .toDF function later
  
  val html = scala.io.Source.fromURL("https://raw.githubusercontent.com/Qiox/Herogi-CaseStudy/main/pace.csv").mkString // Get all rows as one string
  val html2 = scala.io.Source.fromURL("https://raw.githubusercontent.com/Qiox/Herogi-CaseStudy/main/users.csv").mkString // Get all rows as one string

  val m1 = html.split("\\s") // Split based on white spaces at the end of all rows except the last one
                 .filter(_ != "") // Filter out any empty lines
                 .filter(! _.contains("userid"))
                 .filter(! _.contains("total_time"))
                 .filter(! _.contains("distance"))
                 .toSeq // Convert to Seq so we can convert to DF later
                 .map(row => row.split(",")) // Split each line on ',' to make an Array of 3 String each
                 .map { case Array(f1,f2,f3) => (f1,f2,f3) }// Convert that Array[String] into Array[(String, String, String)]               
 
  val m2 = html2.split("\\s") // Split based on white spaces at the end of all rows except the last one
                 .filter(_ != "") // Filter out any empty lines
                 .filter(! _.contains("userid"))
                 .filter(! _.contains("username"))
                 .filter(! _.contains("age"))
                 .toSeq // Convert to Seq so we can convert to DF later
                 .map(row => row.split(",")) // Split each line on ',' to make an Array of 3 String each
                 .map { case Array(f1,f2,f3) => (f1,f2,f3) }// Convert that Array[String] into Array[(String, String, String)]  
                  
  val df1 = m1.toDF("userid", "total_time", "distance") //Creating Pace dataframe
  val df2 = m2.toDF("userid", "username", "age") //Creating Users dataframe
  
  import org.apache.spark.sql.functions._  //For SQL operations
  
  val df3 = df2.join(df1, ("userid")).drop("userid")  //Joining Pace and Users dataframe by their mutual userid column
  val result = df3.withColumn("average_pace", format_number(($"distance".as[Double] / $"total_time".as[Double]),2)).sort(desc("average_pace"))  //Calculating the average pace and getting rid of extra decimals
                                                                                                                                                //while creating the coresponding column and add it to the final dataframe
  
  df1.show()
  df2.show()
  df3.show()
  result.show()

  spark.close() // don't forget to close(), otherwise scastie won't let you create another session so soon.
}
