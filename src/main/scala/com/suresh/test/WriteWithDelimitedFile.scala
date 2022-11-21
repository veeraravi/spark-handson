package com.suresh.test

case class Person(name: String, age: Int, personid: Int)
case class Profile(profileName: String, personid: Int, profileDescription: String)
object WriteWithDelimitedFile {
/* def main(args: Array[String]): Unit = {

   val spark = SparkSession
     .builder()
     .appName("Event Hub Test")
     .config("hive.exec.dynamic.partition", true)
     .config("hive.exec.dynamic.partition.mode", "nonstrict")
     .config("hive.exec.parallel", true)
     .config("hive.enforce.bucketing", true)
     // .config("spark.sql.shuffle.partitions",shufflePartition)
     .config("spark.ui.showConsoleProgress", false)
     .enableHiveSupport()
     .getOrCreate()
   val personDataFrame = spark.sqlContext.createDataFrame(
     Person("Nataraj", 45, 2)
       :: Person("Srinivas", 45, 5)
       :: Person("Ashik", 22, 9)
       :: Person("Deekshita", 22, 8)
       :: Person("Siddhika", 22, 4)
       :: Person("Madhu", 22, 3)
       :: Person("Meghna", 22, 2)
       :: Person("Snigdha", 22, 2)
       :: Person("Harshita", 22, 6)
       :: Person("Ravi", 42, 0)
       :: Person("Ram", 42, 9)
       :: Person("Chidananda Raju", 35, 9)
       :: Person("Sreekanth Doddy", 29, 9)
       :: Nil)
   val profileDataFrame = spark.sqlContext.createDataFrame(
     Profile("Spark", 2, "SparkSQLMaster")
       :: Profile("Spark", 5, "SparkGuru")
       :: Profile("Spark", 9, "DevHunter")
       :: Profile("Spark", 3, "Evangelist")
       :: Profile("Spark", 0, "Committer")
       :: Profile("Spark", 1, "All Rounder")
       :: Nil
   )
   println("Case 1:  First example inner join  ")
   val df_asPerson = personDataFrame.as("dfperson")
   val df_asProfile = profileDataFrame.as("dfprofile")
   val joined_df = df_asPerson.join(
     df_asProfile
     , col("dfperson.personid") === col("dfprofile.personid")
     , "inner")
   // you can do alias to refer column name with aliases to  increase readability
   joined_df.select(
     col("dfperson.name")
     , col("dfperson.age")
     , col("dfprofile.profileName")
     , col("dfprofile.profileDescription"))
     .show

   joined_df.write.option("sep", "|").option("header", "true").csv("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\delimited\\results.txt")
 }*/
}