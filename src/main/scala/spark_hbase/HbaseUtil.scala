/*
package spark_hbase

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

class HbaseUtil {
  val conf = HBaseConfiguration.create()

  System.setProperty("user.name", "hdfs")
  System.setProperty("HADOOP_USER_NAME", "hdfs")
  conf.set("hbase.master", "localhost:60000")
  conf.setInt("timeout", 120000)
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("zookeeper.znode.parent", "/hbase-unsecure")
  conf.set(TableInputFormat.INPUT_TABLE, tableName)

  val admin = new HBaseAdmin(conf)

  def readFromHbase(tableName:String)={

    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("Number of Records found : " + hBaseRDD.count())
  }

  def writeToHbase(resultDf:DataFrame,TTABLE:String)={

    //val TTABLE = "pcrsbx1:atest1"
    val hconf = HBaseConfiguration.create()
    val admin = new HBaseAdmin(hconf)
    if (admin.isTableAvailable(TTABLE)) {
      println(s"Deleting $TTABLE")
      admin.disableTable(TTABLE)
      admin.deleteTable(TTABLE)
    }
    println(s"Creating $TTABLE")
    val htable = new HTableDescriptor(TTABLE)
    htable.addFamily(new HColumnDescriptor("j"))
    htable.addFamily(new HColumnDescriptor("d"))
    htable.addFamily(new HColumnDescriptor("key"))
    htable.addFamily(new HColumnDescriptor("r"))
    admin.createTable(htable)
    println("==============after DF=======" + System.currentTimeMillis())
    saveToHBase(resultDf, TTABLE)
    val end = System.currentTimeMillis()
    //System.currentTimeMillis()
    println("==============after SAVETOHBASE =======" + System.currentTimeMillis())
    val endTimeNanos = System.nanoTime()
    val durationSeconds = (end - start)

    println("===========TOTAL TIME ===========" + durationSeconds)

  }

  val cfDataBytes: Array[Byte] = Array('d');
  val cfJsonBytes: Array[Byte] = Array('j');
  val cfKeyBytes: Array[Byte] = Array('k', 'e', 'y');
  val cfRBytes: Array[Byte] = Array('r');

  val cfDataBytes: Array[Byte] = Array('d');
  val cfJsonBytes: Array[Byte] = Array('j');
  val cfRBytes: Array[Byte] = Array('r');
  listOfColFamily : Array[Array[Byte]]

  object claimRow {
    def convert(row: Row,listOfColFamily : Array[Array[Byte]]): (ImmutableBytesWritable, Put) = {

      val cfDataBytes: Array[Byte] = listOfColFamily(0);
      val cfJsonBytes: Array[Byte] = listOfColFamily(1);
      val cfRBytes: Array[Byte] = listOfColFamily(2);

      val p = new Put(Bytes.toBytes(row.getAs("rowkey").toString))
      p.add(cfKeyBytes, Bytes.toBytes("rxClaimId"), Bytes.toBytes(row.getAs("ClaimId").toString))
      p.add(cfKeyBytes, Bytes.toBytes("rxClaimSequenceNumber"), Bytes.toBytes(row.getAs("rxClaimSequenceNumber").toString))
      p.add(cfKeyBytes, Bytes.toBytes("statusCode"), Bytes.toBytes(row.getAs[String]("statusCode").toString))
      p.add(cfKeyBytes, Bytes.toBytes("claimProcessorCode"), Bytes.toBytes(row.getAs("claimProcessorCode").toString))
      p.add(cfDataBytes, Bytes.toBytes("fillDate"), Bytes.toBytes(row.getAs("fillDate").toString))
      p.add(cfDataBytes, Bytes.toBytes("submitDate"), Bytes.toBytes(row.getAs("submitDate").toString))
      p.add(cfRBytes, Bytes.toBytes("prescriptionNumber"), Bytes.toBytes(row.getAs("prescriptionNumber").toString))
      p.add(cfDataBytes, Bytes.toBytes("refillNumber"), Bytes.toBytes(row.getAs("refillNumber").toString))
      p.add(cfDataBytes, Bytes.toBytes("pharmacyId"), Bytes.toBytes(row.getAs("pharmacyId").toString))
      p.add(cfDataBytes, Bytes.toBytes("pharmacyNPIId"), Bytes.toBytes(row.getAs("pharmacyNPIId").toString))
      p.add(cfDataBytes, Bytes.toBytes("prescriberNPI"), Bytes.toBytes(row.getAs("prescriberNPI").toString))
      p.add(cfDataBytes, Bytes.toBytes("pharmacyNCPDP"), Bytes.toBytes(row.getAs("pharmacyNCPDP").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacyClaimIdentifier"), Bytes.toBytes(row.getAs("pharmacyClaimIdentifier").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacyClaimVersion"), Bytes.toBytes(row.getAs("pharmacyClaimVersion").toString))
      p.add(cfJsonBytes, Bytes.toBytes("medicarePartDClaimVersion"), Bytes.toBytes(row.getAs("medicarePartDClaimVersion").toString))
      p.add(cfJsonBytes, Bytes.toBytes("manualSubmissionClaimVersion"), Bytes.toBytes(row.getAs("manualSubmissionClaimVersion").toString))
      p.add(cfJsonBytes, Bytes.toBytes("memberResponsibleCost"), Bytes.toBytes(row.getAs("memberResponsibleCost").toString))
      p.add(cfRBytes, Bytes.toBytes("totalClaimCost"), Bytes.toBytes(row.getAs("totalClaimCost").toString))
      p.add(cfRBytes, Bytes.toBytes("synchronizedClaimTotalClaimCost"), Bytes.toBytes(row.getAs("synchronizedClaimTotalClaimCost").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacyClaimClarification"), Bytes.toBytes(row.getAs("pharmacyClaimClarification").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacyProvider"), Bytes.toBytes(row.getAs("pharmacyProvider").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacyNetwork"), Bytes.toBytes(row.getAs("pharmacyNetwork").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacySuperNetwork"), Bytes.toBytes(row.getAs("pharmacySuperNetwork").toString))
      p.add(cfJsonBytes, Bytes.toBytes("prescribedProduct"), Bytes.toBytes(row.getAs("prescribedProduct").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmaceuticalProduct"), Bytes.toBytes(row.getAs("pharmaceuticalProduct").toString))
      p.add(cfJsonBytes, Bytes.toBytes("memberPaymentReduction"), Bytes.toBytes(row.getAs("memberPaymentReduction").toString))
      p.add(cfJsonBytes, Bytes.toBytes("pharmacyClaimMessage"), Bytes.toBytes(row.getAs("pharmacyClaimMessage").toString))

      (new ImmutableBytesWritable, p)
    }
  }
  //

  def saveToHBase(df: DataFrame, tableName: String) = {
    val conf = HBaseConfiguration.create()
    // set JobConfiguration variables for writing to HBase
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    //  jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/ptb07962/out")
    //
    // set the HBase output table
    //  jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    println("==============Hbase =======" + System.currentTimeMillis())
    val ClaimRowRDD = df.rdd
    val listOfColFamily : Array[Array[Byte]] = Array(Array('d'),Array('j'),Array('r'))
    ClaimRowRDD.map {
      case row => claimRow.convert(row,listOfColFamily)
    }.saveAsHadoopDataset(jobConfig)
  }

}

*/
