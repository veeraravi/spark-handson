package sparkcore

class MyPartitioner(partitions: Int) extends org.apache.spark.Partitioner{
  override def numPartitions = partitions

  override def getPartition(key: Any):Int ={
    val tempString = key.toString
    if(null != tempString && !tempString.isEmpty){
      val i = tempString.toLowerCase.charAt(0) - 'a'
      if (i < 26 && i >= 0) return i
      else return 0
    }else return 0

  }
}
