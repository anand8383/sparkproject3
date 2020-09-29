package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object getDATAfromWEBURL {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getDATAfromWEBURL").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val result = scala.io.Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
    //Getting  data as string()
    val jsonResponseOneLine = result.toString().stripLineEnd
    //convert string into RDD
    val jsonRdd = sc.parallelize(jsonResponseOneLine :: Nil)
    System.setProperty("http.agent", "Chrome")
    //Convert RDD to Dataframe
    val jsonDf = spark.read.format("json").option("multiLine","true").json(jsonRdd)
    jsonDf.printSchema()
    //using explode breaking the schema
    val formatjson = jsonDf.withColumn("RootArray",explode(col("results"))).select("nationality","RootArray.user.cell","RootArray.user.dob",
      "RootArray.user.email","RootArray.user.location.city","RootArray.user.location.state","RootArray.user.location.street","RootArray.user.location.zip",
      "RootArray.user.md5","RootArray.user.name.first","RootArray.user.name.last","RootArray.user.name.title","RootArray.user.password","RootArray.user.phone","RootArray.user.picture.large",
      "RootArray.user.picture.medium","RootArray.user.picture.thumbnail","RootArray.user.registered","RootArray.user.salt","RootArray.user.sha1","RootArray.user.sha256","RootArray.user.username","seed")
      //printing the result after break the schema
    formatjson.show()
    //printing the schema
    formatjson.printSchema()
    //writing data as CSV in a local drive
    formatjson.write.format("csv").option("header","true").mode("append").save("file:///C:///Users//anand//Desktop//Myfolder//Bigdata//datasets//getURLjsondataSAVEDasCSV")
    println("written done")


    spark.stop()
  }
}

