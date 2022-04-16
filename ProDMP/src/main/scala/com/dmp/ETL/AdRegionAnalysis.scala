package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.kudu.spark.kudu._
/**
  * Created by angel
  */
object AdRegionAnalysis extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.AdRegionAnalysis
  val kuduOptions:Map[String , String] = Map(
    "kudu.table" -> SOURCE_TABLE ,
    "kudu.master" -> KUDU_MASTER
  )

  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {


    //1 ：将ods数据从kudu中取出
    val data: DataFrame = sQLContext.read.options(kuduOptions).kudu
    data.registerTempTable("ods")
    //2：做报表
    val sqlTemp = sQLContext.sql(ContantsSQL.adRegionAnalysis_tmp)
    sqlTemp.registerTempTable("regionTemp")
    val result: DataFrame = sQLContext.sql(ContantsSQL.adRegionAnalysis)

    //3：定义schema
    val schema:Schema = ContantsSchema.AdRegionAnalysis
    val partitionID = "provincename"
    //4：数据落地
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)

  }
}
