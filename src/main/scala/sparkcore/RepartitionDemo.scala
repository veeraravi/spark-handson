package sparkcore

object RepartitionDemo {
  def main(args: Array[String]): Unit = {
    /*
     scala> numRdd.getNumPartitions
res8: Int = 3

scala> numRdd.repartition(2)
res9: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[19] at repartition at <console>:29

scala> numRdd.getNumPartitions
res10: Int = 3

scala> val rep = numRdd.repartition(2)
rep: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[23] at repartition at <console>:28

scala> rep.getNumPartitions
res11: Int = 2

scala> rep.glom.collect
res12: Array[Array[Int]] = Array(Array(1, 3, 5, 7, 9, 11, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34), Array(2, 4, 6, 8, 10, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35))

scala> rep.toDebugString
res13: String =
(2) MapPartitionsRDD[23] at repartition at <console>:28 []
 |  CoalescedRDD[22] at repartition at <console>:28 []
 |  ShuffledRDD[21] at repartition at <console>:28 []
 +-(3) MapPartitionsRDD[20] at repartition at <console>:28 []
    |  ParallelCollectionRDD[13] at parallelize at <console>:26 []

scala> val rep6 = numRdd.repartition(6)
rep6: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[28] at repartition at <console>:28

scala> rep6.getNumPartitions
res14: Int = 6

scala> rep6.glom.collect
res15: Array[Array[Int]] = Array(Array(6, 14, 20, 25, 31), Array(1, 7, 15, 21, 26, 32), Array(2, 8, 16, 22, 27, 33), Array(3, 9, 17, 23, 28, 34), Array(4, 10, 12, 18, 29, 35), Array(5, 11, 13, 19, 24, 30))

scala> rep6.toDebugString
res16: String =
(6) MapPartitionsRDD[28] at repartition at <console>:28 []
 |  CoalescedRDD[27] at repartition at <console>:28 []
 |  ShuffledRDD[26] at repartition at <console>:28 []
 +-(3) MapPartitionsRDD[25] at repartition at <console>:28 []
    |  ParallelCollectionRDD[13] at parallelize at <console>:26 []

scala> val coal2 = numRdd.coalesce(2)
coal2: org.apache.spark.rdd.RDD[Int] = CoalescedRDD[30] at coalesce at <console>:28

scala> coal2.glom.collect
res17: Array[Array[Int]] = Array(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Array(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35))

scala> numRdd.glom.collect
res18: Array[Array[Int]] = Array(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Array(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23), Array(24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35))

scala> coal2.toDebugString
res19: String =
(2) CoalescedRDD[30] at coalesce at <console>:28 []
 |  ParallelCollectionRDD[13] at parallelize at <console>:26 []

scala> val coal2 = numRdd.coalesce(6)
coal2: org.apache.spark.rdd.RDD[Int] = CoalescedRDD[33] at coalesce at <console>:28

scala> coal2.glom.collect
res20: Array[Array[Int]] = Array(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Array(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23), Array(24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35))

scala> coal2.toDebugString
res21: String =
(3) CoalescedRDD[33] at coalesce at <console>:28 []
 |  ParallelCollectionRDD[13] at parallelize at <console>:26 []

scala>*/
  }
}
