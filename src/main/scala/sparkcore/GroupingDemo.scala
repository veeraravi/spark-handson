package sparkcore

import org.apache.spark.sql.SparkSession

object GroupingDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("Group transfermation-2").getOrCreate()
    val sc = spark.sparkContext
    val animals = List("dog","cat","tiger","lion","rabit",
                       "snake","fox","fish","dragan","wolf","cub")
    val animalRdd = sc.parallelize(animals)

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11,12,13,14,15,16,17,18,19,20,
      21,22,23,24,25,26,27,28,29,30,
      31,32,33,34,35)
    val numRdd = sc.parallelize(data,3)

    val grouByRdd = animalRdd.groupBy(ele => ele.charAt(0))
    val evnoddRdd = numRdd.groupBy(ele => if(ele%2==0) "even" else "odd")
//keyBy
    val pairRdd = animalRdd.map(ele => (ele.length,ele))
    val pairRdd2 = animalRdd.keyBy(_.length)
    pairRdd2.groupByKey.collect
    pairRdd2.groupByKey.glom.collect
    pairRdd2.getNumPartitions
    pairRdd2.groupByKey.getNumPartitions

    // reduceBykey
    /*("apple",[1,1,1,1,1])
    ("spark",[1,1,1,1])

     */
    //new HashPartitioner()
    val animals2 = List("dog","cat","tiger","lion","rabit",
      "snake","fox","fish","dragan","wolf","cub","dog","cat","tiger","lion","rabit",
      "snake","fox","fish","dragan","wolf","cub","dog","cat","tiger","lion","rabit",
      "snake","fox","fish","dragan","wolf","cub","dog","cat","tiger","lion","rabit",
      "snake","fox","fish","dragan","wolf","cub")
    val animalRdd2 = sc.parallelize(animals2)

    val pairRdd3 = animalRdd2.map(ele => (ele,1))
    val reduceByKeyRdd = pairRdd3.reduceByKey((x,y)=>x+y)

    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words,3).map(word => (word, 1))
    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _).collect()
    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum))
      .collect()
  }
}
