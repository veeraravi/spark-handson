/*
package com.suresh.test
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object LSBBT_LoopLengthDelta48Extract {

  def main(args: Array[String]) {

    val logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    if (args.length < 3) {
      System.err.println(
        "Usage: LoopLengthHistoricalExtract <start_date> <end_date> <env> " +
          " In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    val last_run_Date_lscrm = args(0)
    val last_run_Date_lsbbt = args(1)
    val env = args(2)


    val spark = SparkSession
      .builder()
      .appName("Ovals Nsp lsbbt delta extracts")
      .enableHiveSupport()
      .getOrCreate()

    var exitcodestatus = "pass"
    try{

      /*********************************************************************************************************************************
        *****       Prepare ban_addressid mappings by joining lscrm tables ie address_table, table_site, table_bus_org      *************
        **********************************************************************************************************************************/

      val latest_ban_listDF = spark.sql( """select objid , org_id as ban,
              (select objid, org_id, row_number() over (partition by objid, org_id order by update_stamp desc) as rnk
               from lscrm_gold.table_bus_org ) tmp  where rnk=1 """)

      val distinct_table_siteDF = spark.sql(" select primary2bus_org , cust_shipaddr2address from (select primary2bus_org , cust_shipaddr2address, row_number() over ( partition by primary2bus_org order by cast(cust_shipaddr2address as double) desc) as rnk from lscrm_gold.table_site) where rnk=1 ")
      val ban_site_mapDF = latest_ban_listDF.join (distinct_table_siteDF,  latest_ban_listDF("ban") === distinct_table_siteDF("primary2_bus_org"), "inner")
        .select("ban","cust_shipaddr2addr")

      val table_addressDF = spark.sql(" select distinct objid, x_amnq_address_id as address_id from lscrm_gold.table_address where x_amnq_address_id !='NULL' " )

      val ban_addr_mapDF =  ban_site_mapDF.join(table_addressDF, ban_site_mapDF("cust_shipaddr2addr") === table_addressDF("objid"), "inner").select("ban", "address_id")


      /*****************************************************************************************************************************************************
        *****             Find all new LL's in last 48 hours and provide NEW_AVG_LL's which are greater than 50 feet from previously sent avg to ovals *****
        ****************************************************************************************************************************************************/

      // Retreive history of 5 ll's per ban , new ll's in 48 hours   and union them
      val hist_5_ll_per_ban   = spark.sql(" select ban, loop_length, data_dt from lsbbt_gold.hist_5_ll_per_ban ")
      val newOrMod_ll_48hrsDF = spark.sql(s""" select distinct ban, ewl as loop_length, data_dt from lsbbt_gold.uniform_7330 where network_type in ('FTTN','FTTN-BP','IP-RT','IP-RT-BP') and data_dt > '${last_run_date_lsbbt}' """)
      val hist_newOrMod_5ll_per_ban = hist_5_ll_per_ban.union(newOrMod_ll_48hrsDF)
      hist_newOrMod_5ll_per_ban.createOrReplaceTempView("hist_newOrMod_5ll_per_ban")

      // Find first 5 ll per ban from newly prepared ll's

      val hist_newOrMod_5_ll_per_banDF = spark.sql(" select ban, loop_length from (select ban, loop_length, data_dt ,row_number() over ( partition by ban order by data_dt desc) as rnk from hist_newOrMod_5ll_per_ban) tmp where rnk <=5 and and cast(ewl as double) < 99999 " )

      // REtreive avg_loop_length's per ban sent to ovals sofar and compare against new avgs and keep if their difference is more than 50

      val hist_newOrMod_avg_ll_banDF = hist_newOrMod_5_ll_per_banDF.groupBy("ban").agg(avg("loop_length").as("new_avg_loop_length"))

      val hist_avg_ll_bp_ind_senttoovalsDF = spark.sql(""" select ban, avg_loop_length from (select ban,avg_loop_length, row_number() over (partition by ban order by  extract_sent_date desc) rnk
          from lsbbt_gold.hist_avg_ll_bp_ind_senttoovalsDF ) tmp where rnk=1 """)

      val newOrMod_avgll_gt_50feetToBeSentToOvalsDF = newOrMod_ll_48hrsDF.join(hist_avg_ll_bp_ind_senttoovalsDF, Seq("ban") ,"left" )
        .filter(hist_newOrMod_avg_ll_banDF("new_avg_loop_length") - coalesce(hist_avg_ll_bp_ind_senttoovalsDF("avg_loop_length"), 0) >= 50)
        .select("ban", "new_avg_loop_length")


      /*********************************************************************************************************************************
        ***** Find all new address_id's created in last 48 hours + new LL's with BP changed           *************
        *********************************************************************************************************************************/

      // Retreive historically sent address_id's , new address_id's created (delta) in last 48 hours and union them
      val hist_address_idDF = spark.sql("select address_id from lsbbt_gold.existing_addressid_lscrm ")
      val new_address_idDF = spark.sql("select distinct x_amnq_address_id as address_id from lscrm_gold.address_data  where data_dt > $last_run_date_lscrm ")
      val new_address_id_not_in_historyDF = new_address_idDF.except(hist_address_idDF);   //delta
      val hist_new_addressidDF = new_address_id_not_in_historyDF.union(hist_address_idDF)
      // Retreive historically prepared bp_ind , new aLL's created in last 48 hours with bpind (delta) and union them
      val hist_ban_with_bpDF = spark.sql("select ban, bp_ind from lsbbt_gold.hist_ban_with_bp")
      val newOrMod_ll_48hrsWithbpDF = spark.sql("select distinct ban, 'Y' as bp_ind  from lsbbt_gold.uniform_7330 where  network_type in ('FTTN-BP','IP-RT-BP') and data_dt > '$last_run_Date_lsbbt' ")
      // delta
      val newOrMod_bans_not_in_histbpindDF = newOrMod_ll_48hrsWithbpDF.except(hist_ban_with_bpDF)
      val hist_newOrMod_bans_bp_indDF = hist_ban_with_bpDF.union(newOrMod_bans_not_in_histbpindDF)

      //Join delta address_id's with ban
      val new_addressid_banDF = new_address_id_not_in_historyDF.join(ban_addr_mapDF, Seq("address_id"), "inner")
        .select("ban", "address_id")
      //Join delta LL's with new BP with address_id
      val newOrMod_bans_not_in_histbpind_banDF = newOrMod_bans_not_in_histbpindDF.join(ban_addr_mapDF, Seq("ban"), "inner")
        .select("ban", "address_id")

      //Mandatory list (either new address_id or bp_ind changed) to be sent irrespective of LL change of 50 feet
      val bans_addressid_must_be_sent_toOvalsDF = new_addressid_banDF.union(newOrMod_bans_not_in_histbpind_banDF)  //ban_addressid


      /****************************************************************************************************************************
        *  Join newLL's_48hrs_avgLL , new address_id's, new BP_ind's with
        ******************************************************************************************************************************/

      val newOrMod_ll_gt50feet_addressidDF = newOrMod_avgll_gt_50feetToBeSentToOvalsDF.join(ban_addr_mapDF, Seq("ban"), "inner")
        .join(hist_newOrMod_bans_bp_indDF, Seq("ban"), "left_outer")
        //.withColumn("bp_ind", coalesce(hist_newOrMod_bans_bp_indDF("bp_ind"), "N"))
        .na.fill("N", Seq("bp_ind"))
        .select("ban", "address_id", "new_avg_loop_length", "bp_ind")

      val new_bans_with_new_addressid_Or_bpindD_AVGLL_DF = bans_addressid_must_be_sent_toOvalsDF.join (hist_newOrMod_avg_ll_banDF, Seq("ban"), "inner")
        .join(hist_newOrMod_bans_bp_indDF, Seq("ban"), "left")
        //.withColumn("bp_ind", coalesce(hist_newOrMod_bans_bp_indDF("bp_ind"), "N"))
        .na.fill("N", Seq("bp_ind"))
        .select("ban","address_id","new_avg_loop_length", "bp_ind")



      /*********************************************************************************************************************************
        ******              JOIN All two sets and send the extract                                                        *************
        *********************************************************************************************************************************/

      val finalExtractWithBanDF = new_bans_with_new_addressid_Or_bpindD_AVGLL_DF.union(newOrMod_ll_gt50feet_addressidDF)
      val finalExtractDF = new_bans_with_new_addressid_Or_bpindD_AVGLL_DF.union(newOrMod_ll_gt50feet_addressidDF).drop("ban")
      finalExtractDF.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save(s"/$env/gold/lsbbt_gold/lsbbt_extracts_to_ovalsNSP/delta/")

      /*********************************************************************************************************************************
        ******              UPDATE BACK Existing historical tables with Last 48 hours updates                                *************
        *********************************************************************************************************************************/

      spark.sql("INSERT OVERWRITE TABLE lsbbt_gold.hist_5_ll_per_ban SELECT * from hist_newOrMod_5ll_per_ban")

      hist_newOrMod_bans_bp_indDF.createOrReplaceTempView("hist_newOrMod_bans_bp_indDF")
      spark.sql("INSERT OVERWRITE TABLE lsbbt_gold.hist_ban_with_bp  SELECT * from hist_newOrMod_bans_bp_indDF")

      hist_new_addressidDF.createOrReplaceTempView("hist_new_addressidDF")
      spark.sql("INSERT OVERWRITE TABLE lsbbt_gold.existing_addressid_lscrm  SELECT * from hist_new_addressidDF")

      val today = spark.range(1).select(current_date).select("current_date")
      finalExtractWithBanDF.withColumn("extract_sent_date",current_date)
      finalExtractWithBanDF.createOrReplaceTempView("finalExtractWithBanDF")
      spark.sql("INSERT into TABLE lsbbt_gold.hist_avg_ll_bp_ind_senttoovalsDF  SELECT * from finalExtractWithBanDF")

      // TODO: Need to clean lsbbt_gold.hist_5_11_per_ban table with records > 13 months

    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(" LSBBT Delta extract failed with below stacktrace")
        logger.error(e.printStackTrace().toString())
        exitcodestatus = "fail"
    }
    if (exitcodestatus == "fail") {
      System.exit(1)
    }
  } // end of main
} //end of object*/
