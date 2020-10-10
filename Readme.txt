# Herogi-CaseStudy
Runner Leaderboard

Scala ver: 2.12.12

Spark ver: 3.0.0

Created and worked on: Scastie

<script src="https://scastie.scala-lang.org/8Dntk9VxRvmfvfuvxYdxJw.js"></script>

---------------------------------------------------------------------------------------------------------------------
First of all, I used Scala-Spark and worked on the website called Scastie where you can implement and test your code on the browser environment. I find it really useful and reachable and that's the reason I chose it.
But in order to execute the code, you need to insert the specific dependencies for spark which can be seen at the bottom of the "build.sbt".

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0"
)

-Walkthrough-

1) I needed to read the input files in order to navigate through my mission. There comes my first challenge. Because, Scastie is a browser environment and it  can not reach the files in my computer, therefore I can not simply show a filepath for input files. However, I can show an URL link to Scastie that was on my Github. I converted the raw URL format of the each Pace.csv and Users.csv to a seperate single String. Before starting to process them I split them based on white spaces at the end of each row. (There was a white space for some reason and I don't know why but I used it to my advantage)

2) After that conversion, my aim was converting those strings into workable map format. Before converting I also filtered out the unnecessary parts such as the first elements of those strings (userid, user_name, distance, age, total_time), empty lines and white spaces at the end of every row except the last one.

3) Then the conversion part begins. I converted those strings into a sequence by using .toSeq() function which is utilized for displaying a sequence from the Scala map. Also, splitting each line based on ',' (comma) to make an Array of 3 String each. And finally convert those Array[String] into Array[(String, String, String)] format (we will need this for future dataframe conversion).

4) Now we have 2 delicious arrays ready to be transformed into data-frames by using .toDF() fucntion. This is the place where Spark shines. Thus, "import spark.implicits._" is necessary. Now we have 2 dataframes for each .csv file where "df_pace" refers to pace.csv and "df_user" to users.csv. We also added our desired column names that are "userid", "total_time", "distance" for df_pace and "userid", "username", "age" for df_user.

5) Next step includes sql operations, therefore I implemented "import org.apache.spark.sql.functions._" which was more enough than I need. I encountered many bugs and problems at this very part which is joining the two dataframes with 1 identical column (userid). Most annoying part was however I joined them, their structure was broken in all kind of way. After hours of torment I finally managed to join them by their userid column by solving the issue of "white spaces" thanks to seperating the first html string based on "white spaces". Those white spaces were making the data somehow corrupted and was messing with the arrangement of the layout of dataframe.

6) There were no correlation whatsoever for the "sex" data and they were not included in the input files as well. Because of this and the low number of entries lead me to insert all sex data manually by creating a sex data and assigning them to the usernames. I thought this was the most efficient way to operate considering the current circumstances.

7) Finally, our last step is to take the average pace of every user and sort them in a descending order. I did those two operations in a single line which is " val df_result = df_merge.withColumn("average_pace (km / min)", format_number((($"distance (m)"/1000).as[Double] / $"total_time (min)".as[Double]),2)).sort(desc("average_pace (km / min)")) " where "df_result" is our final desired dataframe. Here, I created a column called "average_pace", then calculated the average pace by dividing distance to total_time (km / minute), inserted the new values into "average_pace" column and finally sorted them in a descending order.

8) Conclusion: We have a console application which shows 4 outputs which are df_pace (pace dataframe), df_user (users dataframe), df_merge (users and pace combined) and result (big dataframe with average pace). Desired output is shown below:

Pace Dataframe
+------+----------------+------------+
|userid|total_time (min)|distance (m)|
+------+----------------+------------+
|     1|              25|        5000|
|     2|              24|        4800|
|     3|              28|        6000|
|     4|              40|       10000|
|     5|              12|        2000|
|     6|              42|       10000|
|     7|              20|        5000|
|     8|              21|        5000|
|     9|              30|        5000|
|    10|              22|        5000|
|    11|              30|        6000|
|    12|              28|        4500|
|    13|              18|        5000|
|    14|              60|       10000|
|    15|              45|        8000|
+------+----------------+------------+

Users Dataframe
+--------+------+---+------+
|username|userid|age|   sex|
+--------+------+---+------+
|   user1|     1| 24|    --|
|   ahmet|     2| 21|  male|
|   yunus|     3| 28|  male|
|    john|     5| 34|  male|
|    adam|     6| 31|  male|
|   brian|     7| 41|  male|
|   tyler|     8| 26|  male|
|   ceren|     9| 35|female|
|   hasan|    10| 43|  male|
|   selen|    11| 29|female|
|   diana|    12| 45|female|
|  thomas|    13| 44|  male|
|   james|    14| 39|  male|
|   janet|    15| 36|female|
+--------+------+---+------+

User + Pace merged dataframe
+--------+---+------+----------------+------------+
|username|age|   sex|total_time (min)|distance (m)|
+--------+---+------+----------------+------------+
|   user1| 24|    --|              25|        5000|
|   ahmet| 21|  male|              24|        4800|
|   yunus| 28|  male|              28|        6000|
|    john| 34|  male|              12|        2000|
|    adam| 31|  male|              42|       10000|
|   brian| 41|  male|              20|        5000|
|   tyler| 26|  male|              21|        5000|
|   ceren| 35|female|              30|        5000|
|   hasan| 43|  male|              22|        5000|
|   selen| 29|female|              30|        6000|
|   diana| 45|female|              28|        4500|
|  thomas| 44|  male|              18|        5000|
|   james| 39|  male|              60|       10000|
|   janet| 36|female|              45|        8000|
+--------+---+------+----------------+------------+

Result dataframe with average pace
+--------+---+------+----------------+------------+-----------------------+
|username|age|   sex|total_time (min)|distance (m)|average_pace (km / min)|
+--------+---+------+----------------+------------+-----------------------+
|  thomas| 44|  male|              18|        5000|                   0.28|
|   brian| 41|  male|              20|        5000|                   0.25|
|    adam| 31|  male|              42|       10000|                   0.24|
|   tyler| 26|  male|              21|        5000|                   0.24|
|   hasan| 43|  male|              22|        5000|                   0.23|
|   yunus| 28|  male|              28|        6000|                   0.21|
|   selen| 29|female|              30|        6000|                   0.20|
|   user1| 24|    --|              25|        5000|                   0.20|
|   ahmet| 21|  male|              24|        4800|                   0.20|
|   janet| 36|female|              45|        8000|                   0.18|
|    john| 34|  male|              12|        2000|                   0.17|
|   james| 39|  male|              60|       10000|                   0.17|
|   ceren| 35|female|              30|        5000|                   0.17|
|   diana| 45|female|              28|        4500|                   0.16|
+--------+---+------+----------------+------------+-----------------------+
