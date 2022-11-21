package com.suresh.test

import java.io.IOException
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LSBBT_LoopLengthHistoricalExtract {

  /* val logger = LogManager.getRootLogger
   logger.setLevel(Level.INFO)
   val log = Logger.getLogger(getClass.getName)*/

  def main(args: Array[String]) {

    var logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    if (args.length < 3) {
      System.err.println(
        "Usage: LoopLengthHistoricalExtract <start_date> <end_date> <env> " +
          " In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    val config = new SparkConf(true)
    val Seq( start_date,end_date, env ) = args.toSeq
    /*val start_date = args(0)
    val end_date = args(1)*/
    val loglevel = args(2)
    //val env = args(2)

    var progprops = new Properties()
    try {
      progprops.load(FileSystem.get(new Configuration()).open(new Path("/opt/data/stage01/lsbbt/conf/spark.properties")))
    } catch {
      case ex: IOException => {
        System.err.println("Unable to fetch Configuration details...")
        logger.error("Unable to fetch Configuration details ...")
        //System.exit(1)
      }
    }
    /* val progprops: Properties = new Properties
     val envConf= new FileInputStream(/opt/data/stage01/lsbbt/conf/spark.properties);
     progprops.load(envConf)*/

    val exec_mem = progprops.getProperty("spark.executor.memory")
    val exec_cores = progprops.getProperty("spark.executor.cores")
    val driv_mem = progprops.getProperty("spark.executor.memory")
    val driv_cores = progprops.getProperty("spark.driver.cores")
    val serializer = progprops.getProperty("spark.serializer")
    val exec_inst =  progprops.getProperty("spark.executor.instances")
    val dflt_parallel =  progprops.getProperty("spark.default.parallelism")
    val cbo_enbl =  progprops.getProperty("spark.sql.cbo.enabled")
    val cbo_joinr =  progprops.getProperty("spark.sql.cbo.joinReorder.enabled")
    val shuf_prt =  progprops.getProperty("spark.sql.shuffle.partitions")
    val debug_maxstr =  progprops.getProperty("spark.debug.maxToStringFields")
    val schdlr_mode =  progprops.getProperty("spark.scheduler.mode")

    val spark = SparkSession.builder().appName("Ovals Nsp lsbbt extracts").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.executor.memory",exec_mem)
    spark.conf.set("spark.executor.cores",exec_cores)
    spark.conf.set("spark.driver.memory",driv_mem)
    spark.conf.set("spark.driver.cores",driv_cores)
    spark.conf.set("spark.sql.cbo.enabled",cbo_enbl)
    spark.conf.set("spark.sql.cbo.joinReorder.enabled",cbo_joinr)
    spark.conf.set("spark.sql.shuffle.partitions",shuf_prt)
    spark.conf.set("spark.debug.maxToStringFields",debug_maxstr)
    spark.conf.set("spark.scheduler.mode",schdlr_mode)
    spark.conf.set("spark.executor.instances",exec_inst)
    spark.conf.set("spark.default.parallelism",dflt_parallel)
    spark.conf.set("spark.serializer",serializer)
    spark.sparkContext.setLogLevel(loglevel)
    logger.info(s"Extract generation started for lsbbt to ovals nsp")

    var exitcodestatus = "pass"
    try{

      /********  Prepare ban_addressid mappings by joining lscrm tables ie address_table, table_site, table_bus_org  *******/

      val latest_ban_listDF = spark.sql( """select objid , org_id as ban from
             (select objid, org_id, row_number() over (partition by objid, org_id order by update_stamp desc) as rnk
              from lscrm_gold.table_bus_org  ) tmp  where rnk=1 """);

      val distinct_table_siteDF = spark.sql(" select primary2bus_org , cust_shipaddr2address from (select primary2bus_org , cust_shipaddr2address, row_number() over ( partition by primary2bus_org order by cast(cust_shipaddr2address as double) desc) as rnk from lscrm_gold.table_site) where rnk=1 ")

      val ban_site_mapDF = latest_ban_listDF.join(distinct_table_siteDF,latest_ban_listDF("objid")===distinct_table_siteDF("primary2bus_org"),"inner").select("ban","cust_shipaddr2address");

      val table_addressDF = spark.sql(" select distinct objid, x_amnq_address_id as address_id from lscrm_gold.table_address where x_amnq_address_id !='NULL' ")

      val ban_addr_mapDF =  ban_site_mapDF.join(table_addressDF, ban_site_mapDF("cust_shipaddr2address")===table_addressDF("objid"), "inner")
        .select("ban", "address_id");




      /************************************************************************************************************************************
        *****         Prepare all historical tables 1.distinct addressid sent in initial extract (code is at the end of process)  **********
        *****                                       2.ban_bondedpair_mapping                                                      **********
        *****                                       3.ban_5_ll_mapping                                                            **********
        *****                       These tables also used in delta extracts going forward                                        **********
        ************************************************************************************************************************************/


      //1. Get list of ban's which were once used to be BondedPair in last 13 months

      val hist_ban_with_bpDF = spark.sql ("""select distinct ban , 'Y' as bp_ind from lsbbt_gold.uniform_7330
                 where network_type in ('FTTN-BP', 'IP-RT-BP' ) and data_dt between "$start_date" and "$end_date" """);

      hist_ban_with_bpDF.createOrReplaceTempView("hist_ban_with_bp");

      spark.sql("create table if not exists lsbbt_gold.hist_ban_with_bp as select * from hist_ban_with_bp");

      spark.sql("INSERT OVERWRITE TABLE lsbbt_gold.hist_ban_with_bp  SELECT * from hist_ban_with_bp")

      //2. Get loop length from last 5 ll's related to each ban in last 13 months

      val hist_5_ll_banDF = spark.sql(""" select ban, loop_length, data_dt from
          (select ban, ewl as loop_length, data_dt ,row_number() over ( partition by ban order by data_dt desc) as rnk
              from (select distinct ban, ewl , data_dt from lsbbt_gold.uniform_7330
                  where network_type in ('FTTN','FTTN-BP','IP-RT','IP-RT-BP') and data_dt between "$start_date" and "$end_date" and ewl < '99999')) tmp
                  where rnk <=5 """);


      hist_5_ll_banDF.createOrReplaceTempView("hist_5_ll_per_ban");

      spark.sql("create table if not exists lsbbt_gold.hist_5_ll_per_ban as select * from hist_5_ll_per_ban");

      spark.sql("INSERT OVERWRITE TABLE lsbbt_gold.hist_5_ll_per_ban SELECT * from hist_5_ll_per_ban")

      /*****   Find avg of last 5 LL's and join with bp_ind   ***************/

      // Get avg loop length from last 5 ll's related to each ban in last 13 months
      //  val hist_avg_ll_banDF = hist_5_ll_banDF.groupBy("ban").agg(avg("loop_length"));

      val hist_avg_ll_banDF = hist_5_ll_banDF.groupBy("ban").agg(avg("loop_length").as("loop_length"))


      //  Join bp_ind and avg table

      val hist_avg_ll_bp_indDF = hist_avg_ll_banDF.join(hist_ban_with_bpDF, Seq("ban"), "inner")
        .select("ban", "loop_length", "bp_ind").na.fill("N", Seq("bp_ind"));



      /*********************************************************************************************************************************
        *****                                            Prepare addressid_LL_mapping                                     *************
        **********************************************************************************************************************************/

      val ban_addr_ll_mapDF = hist_avg_ll_bp_indDF.join(ban_addr_mapDF, Seq("ban"), "inner")
        .select("address_id","loop_length","bp_ind");

      // Create all distinct address_id's sent as part of initial extract (used only in delta file extraction)

      val all_existing_distinct_addressidDF  = ban_addr_ll_mapDF.select("address_id").distinct;

      all_existing_distinct_addressidDF.createOrReplaceTempView("existing_addressid_lscrm");

      spark.sql("create table if not exists lsbbt_gold.existing_addressid_lscrm as select * from existing_addressid_lscrm")

      spark.sql("INSERT OVERWRITE TABLE lsbbt_gold.existing_addressid_lscrm  SELECT * from existing_addressid_lscrm")

      // Saving the final extract data in hdfs path for publish

      ban_addr_ll_mapDF.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("/$env/gold/lsbbt_gold/lsbbt_extracts_to_ovalsNSP/initial/");

      val outputPath="/user/veera/1"
      val filename="result.csv"
      val outputfilename=outputPath+"/temp_"+filename
      val mergedfile=outputPath+"/merged_"+filename

      val mergedfineglob =outputfilename


      def mergeTextFiles(srcPath: String, dstPath: String, deleteSource: Boolean): Unit =  {
        import org.apache.hadoop.fs.FileUtil
        import java.net.URI
        val config = spark.sparkContext.hadoopConfiguration
        val fs: FileSystem = FileSystem.get(new URI(srcPath), config)
        FileUtil.copyMerge(
          fs, new Path(srcPath), fs, new Path(dstPath), deleteSource, config, null
        )

      }
      mergeTextFiles(mergedfineglob,mergedfile,true)



    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(" LSBBT Historical extract failed with below stacktrace")
        logger.error(e.printStackTrace().toString())
        exitcodestatus = "fail"
    }
    if (exitcodestatus == "fail") {
      System.exit(1)
    }

  } // end of main
} //end of object