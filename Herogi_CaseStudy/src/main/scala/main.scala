import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.util.{Try, Success, Failure}
import scala.math.BigDecimal

object TestApp extends App {
  lazy implicit val spark = SparkSession.builder().master("local").appName("spark_test").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._ // Required to call the .toDF function later
  
  val html_pace = scala.io.Source.fromURL("https://raw.githubusercontent.com/Qiox/Herogi-CaseStudy/main/pace.csv").mkString // Get all rows as one string
  val html_user = scala.io.Source.fromURL("https://raw.githubusercontent.com/Qiox/Herogi-CaseStudy/main/users.csv").mkString // Get all rows as one string

  val map_pace = html_pace.split("\\s") // Split based on white spaces at the end of all rows except the last one
                 .filter(_ != "") // Filter out any empty lines
                 .filter(! _.contains("userid"))
                 .filter(! _.contains("total_time"))
                 .filter(! _.contains("distance"))
                 .toSeq // Convert to Seq so we can convert to DF later
                 .map(row => row.split(",")) // Split each line on ',' to make an Array of 3 String each
                 .map { case Array(f1,f2,f3) => (f1,f2,f3) }// Convert that Array[String] into Array[(String, String, String)]               
 
  val map_user = html_user.split("\\s") // Split based on white spaces at the end of all rows except the last one
                 .filter(_ != "") // Filter out any empty lines
                 .filter(! _.contains("userid"))
                 .filter(! _.contains("username"))
                 .filter(! _.contains("age"))
                 .toSeq // Convert to Seq so we can convert to DF later
                 .map(row => row.split(",")) // Split each line on ',' to make an Array of 3 String each
                 .map { case Array(f1,f2,f3) => (f1,f2,f3) }// Convert that Array[String] into Array[(String, String, String)]  
                  
  val df_pace = map_pace.toDF("userid", "total_time (min)", "distance (m)") //Creating Pace dataframe
  val df_user = map_user.toDF("userid", "username", "age")  //Creating Users dataframe
  
  import org.apache.spark.sql.functions._  //For SQL operations
  val sex = Seq(("user1","--"),("ahmet","male"),("yunus","male"),("john","male"),("adam","male"),("brian","male"),("tyler","male"),("ceren","female"),("hasan","male"),("selen","female"),("diana","female"),("thomas","male"),("james","male"),("janet","female"))
  val df_sex = sex.toDF("username","sex")  //Creating sex data for every user
  val df_user_sex = df_user.join(df_sex, ("username"))  //Adding sex data into user dataframe
  val df_merge = df_user_sex.join(df_pace, ("userid")).drop("userid")  //Joining Pace and Users dataframe by their mutual userid column
  val df_result = df_merge.withColumn("average_pace (km / min)", format_number((($"distance (m)"/1000).as[Double] / $"total_time (min)".as[Double]),2)).sort(desc("average_pace (km / min)"))  //Calculating the average pace and getting rid of extra decimals
                                                                                                                                                                                       //while creating the coresponding column and add it to the final dataframe
  
  df_pace.show()
  df_user_sex.show()
  df_merge.show()
  df_result.show()

  spark.close() // don't forget to close(), otherwise scastie won't let you create another session so soon.
}
