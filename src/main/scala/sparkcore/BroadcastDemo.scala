package sparkcore

import org.apache.spark.sql.SparkSession

object BroadcastDemo {
def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
                              .master("local[*]")
                              .appName("WordCount Example")
                              .getOrCreate()
    val sc = spark.sparkContext
    // if you are reading from localfilesystem it will consider a
    // blocksize as partiton i.e 32mb is one block
    //
  val stopwords = Array("a","about","above","after","again","against","all",
      "am","an","and","any","are","aren't","as","at","be","because","been",
      "before","being","below","between","both","but","by","can't","cannot",
      "could","couldn't","did","didn't","do","does","doesn't","doing",
      "don't","down","during","each","few","for","from","further","had",
      "hadn't","has","hasn't","have","haven't","having","he","he'd","he'll",
      "he's","her","here","here's","hers","herself","him","himself","his",
      "how","how's","i","i'd","i'll","i'm","i've","if","in","into","is",
      "isn't","it","it's","its","itself","let's","me","more","most","mustn't"
      ,"my","myself","no","nor","not","of","off","on","once","only","or",
      "other","ought","our","ours","ourselves","out","over","own","same",
      "where","where's","which","while","who","who's","whom","why","why's",
      "with","won't","would","wouldn't","you","you'd","you'll","you're",
      "you've","your","yours","yourself","yourselves")

  val stopWordMap = collection.mutable.Map[String,Int]()
  for(word <- stopwords){
    stopWordMap(word)=1
     }
  val bcWords = sc.broadcast(stopWordMap)

  def convertWords(line:String):Array[String]={
    var words = line.split(" ")
    var outVar = Array[String]()
    for(word <- words){
      if(!(bcWords.value contains word.toLowerCase.trim.replaceAll("[^a-z]",""))){
        outVar = outVar:+word;
      }
    }
    return outVar;
  }


    val textRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt")

    println(":: number of partition :: "+textRDD.getNumPartitions)

    val rdd2 = textRDD.flatMap(convertWords)

    rdd2.foreach(ele => println(ele))

    val rdd3 = rdd2.map(ele => (ele,1))

    rdd3.foreach(ele => println(ele))

    val countWords = rdd3.reduceByKey((a,b) => a+b).persist()



    countWords.foreach(ele => println(ele))
    countWords.saveAsTextFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\wordCount16\\")
    println(" DAG PLAN "+countWords.toDebugString)

  sc.stop()


  }
}
