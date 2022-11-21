package sparkcore

import org.apache.spark.sql.SparkSession

object BasicTransfermatonPart2Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("Basic transfermation-2").getOrCreate()
    val sc = spark.sparkContext
    val animals = List("dog","cat","tiger","lion","rabit","snake","fox")
    val rdd = sc.parallelize(animals)
    println("===iterate through RDD====")
    rdd.foreach(println)
    rdd.foreach(ele => println("Animal == "+ele))
    rdd.foreachPartition(println)

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                            11,12,13,14,15,16,17,18,19,20,
                          21,22,23,24,25,26,27,28,29,30,
                          31,32,33,34,35)
    val numRdd = sc.parallelize(data)

    numRdd.foreachPartition(ele => println(ele.reduce(_+_)))
    numRdd.foreachPartition(ele => println(ele.fold(0)(_+_)))
  // rdd.glom.collect
  //  res11: Array[Array[Int]] = Array(
  // Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12, 13),
  // Array(14, 15, 16, 17), Array(18, 19, 20, 21),
  // Array(22, 23, 24, 25, 26), Array(27, 28, 29, 30),
  // Array(31, 32, 33, 34, 35))
    //mapPartitions
    val mapTrans = numRdd.map(sumFuncMap)
    val mapTrans2 = numRdd.map(ele => 0+ele)
//Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
// 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35)

    //mapPartitions
    val mappartiTrans = numRdd.mapPartitions(sumFuncMapPartition)
//op:  Array[Int] = Array(10, 26, 55, 62, 78, 120, 114, 165)
//mapPartitionWithIndex

    val res = numRdd.mapPartitionsWithIndex((index:Int,iter:Iterator[Int])=>
      iter.toList.map(ele => if(index == 2)print(ele)).iterator
    )
res.foreach(println)
  }

  def sumFuncMap(nums:Int):Int ={
    var sum=0;
    sum = sum+nums;
  return  sum
  }

  def sumFuncMapPartition(nums:Iterator[Int]):Iterator[Int]={
    var sum =0;
    while(nums.hasNext){
      sum = sum+nums.next()
    }
 return   Iterator(sum)
  }

}
