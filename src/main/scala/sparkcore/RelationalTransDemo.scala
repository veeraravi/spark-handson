package sparkcore

import org.apache.spark.sql.SparkSession

object RelationalTransDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(" Relational Trans Demo")
                  .master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val fruits = List("banana","apple","orange","Kiwi","mango","apple","Kiwi","goa","pineApple")
val fRdd = sc.parallelize(fruits,4);
   // fRdd.sortBy()
    val frutiLength = fRdd.map(ele => (ele, ele.length))
    val sortedRdd = frutiLength.sortByKey()
    /*
    scala> sortedRdd.collect
res17: Array[(String, Int)] = Array((Kiwi,4), (Kiwi,4), (apple,5), (apple,5), (banana,6), (goa,3), (mango,5), (orange,6), (pineApple,9))

scala>     val sortedRdd = frutiLength.sortByKey(false)
sortedRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[30] at sortByKey at <console>:34

scala> sortedRdd.collect
res18: Array[(String, Int)] = Array((pineApple,9), (orange,6), (mango,5), (goa,3), (banana,6), (apple,5), (apple,5), (Kiwi,4), (Kiwi,4))

scala>     val sortedRdd = frutiLength.sortByKey(false,2)
sortedRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[33] at sortByKey at <console>:34

scala> sortedRdd.getNumPartitions
res19: Int = 2

scala> sortedRdd.collect
res20: Array[(String, Int)] = Array((pineApple,9), (orange,6), (mango,5), (goa,3), (banana,6), (apple,5), (apple,5), (Kiwi,4), (Kiwi,4))
     */
  // sortBy
/*
scala> val srt = fRdd.sortBy(ele => ele, true)
srt: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[38] at sortBy at <console>:32

scala> srt.collect
res21: Array[String] = Array(Kiwi, Kiwi, apple, apple, banana, goa, mango, orange, pineApple)

scala> srt.getNumPartitions
res22: Int = 4

scala> fRdd.getNumPartitions
res23: Int = 4

scala> val srt2 = fRdd.sortBy(ele => ele, false,2)
srt2: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[43] at sortBy at <console>:32

scala> srt2.getNumPartitions
res24: Int = 2

scala> srt2.collect
res25: Array[String] = Array(pineApple, orange, mango, goa, banana, apple, apple, Kiwi, Kiwi)

 scala>     val srt3 = frutiLength.sortBy(ele => ele._1,true)
srt3: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[52] at sortBy at <console>:34

scala> srt3.collect
res29: Array[(String, Int)] = Array((Kiwi,4), (Kiwi,4), (apple,5), (apple,5), (banana,6), (goa,3), (mango,5), (orange,6), (pineApple,9))

scala>     val srt3 = frutiLength.sortBy(ele => ele._2,true)
srt3: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[57] at sortBy at <console>:34

scala> srt3.collect
res30: Array[(String, Int)] = Array((goa,3), (Kiwi,4), (Kiwi,4), (apple,5), (mango,5), (apple,5), (banana,6), (orange,6), (pineApple,9))

 */
    //join

    val seedlessFruit = List("grapes","banana","pineApple","pear","honeyDue","kiwi")
    val slfruitRdd = sc.parallelize(seedlessFruit,1)
    val slflengthRdd = slfruitRdd.map(ele => (ele,ele.length))
    val seededFruit = List("watermelon","muskmelon","apple","mango","dates",
                           "orange","kiwi","pear","grapes")
    val sfruitRdd = sc.parallelize(seededFruit,1)
    val sflengthRdd = sfruitRdd.map(ele => (ele,ele.length))

    val joinedRdd = slflengthRdd.join(sflengthRdd)
    joinedRdd.glom().collect

    slflengthRdd.leftOuterJoin(sflengthRdd)

    // Union
    slfruitRdd.union(sfruitRdd).collect
    /*  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: RDD[T]): RDD[T] = withScope {
    sc.union(this, other)
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }
*/
    // remove duplicate in UNION
    slfruitRdd.union(sfruitRdd).distinct.collect

    // intersection
    slfruitRdd.intersection(sfruitRdd).glom.collect
   /* def intersection(other: RDD[T]): RDD[T] = withScope {
      this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
    }

    /**
      * Return the intersection of this RDD and another one. The output will not contain any duplicate
      * elements, even if the input RDDs did.
      *
      * @note This method performs a shuffle internally.
      *
      * @param partitioner Partitioner to use for the resulting RDD
      */
    def intersection(
                      other: RDD[T],
                      partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
      this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
    }

    /**
      * Return the intersection of this RDD and another one. The output will not contain any duplicate
      * elements, even if the input RDDs did.  Performs a hash partition across the cluster
      *
      * @note This method performs a shuffle internally.
      *
      * @param numPartitions How many partitions to use in the resulting RDD
      */
    def intersection(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
      intersection(other, new HashPartitioner(numPartitions))
    }
*/
    //subtract A-B
   /* scala> slfruitRdd.collect
    res15: Array[String] = Array(grapes, banana, pineApple, pear, honeyDue, kiwi)

    scala> sfruitRdd.collect
    res16: Array[String] = Array(watermelon, muskmelon, apple, mango, dates, orange,
                                 kiwi, pear, grapes)

    slfruitRdd.subtract(sfruitRdd).glom.collect
res12: Array[Array[String]] = Array(Array(banana, pineApple, honeyDue))


*/
    slfruitRdd.subtract(sfruitRdd).glom.collect

    // cartesian
    val rddSetA = sc.parallelize(List(1,2,3,4))
    val rddSetB = sc.parallelize(List(5,6,7,8))

    rddSetA.cartesian(rddSetB).collect().foreach(println)


    /*scala> rddSetA.cartesian(rddSetB).count()
res1: Long = 16

scala> rddSetA.cartesian(rddSetB).take(2)
res2: Array[(Int, Int)] = Array((1,5), (1,6))

scala> rddSetA.cartesian(rddSetB).first()
res3: (Int, Int) = (1,5)

scala> rddSetA.cartesian(rddSetB).head()
<console>:29: error: value head is not a member of org.apache.spark.rdd.RDD[(Int, Int)]
       rddSetA.cartesian(rddSetB).head()
                                  ^

scala> rddSetA.cartesian(rddSetB).tail()
<console>:29: error: value tail is not a member of org.apache.spark.rdd.RDD[(Int, Int)]
       rddSetA.cartesian(rddSetB).tail()
                                  ^

scala> val rddA = sc.parallelize(List(2,3,1,4,8,6,2,10,32,7))
rddA: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[6] at parallelize at <console>:24

scala> rddA.takeOrdered(3)
res6: Array[Int] = Array(1, 2, 2)
*/
  }
}
