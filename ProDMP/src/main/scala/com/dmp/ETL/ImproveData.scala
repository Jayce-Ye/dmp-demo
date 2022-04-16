package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.bean.La_lo_region_city
import com.dmp.tools._
import com.dmp.tools.ips.ParseIp2Bean
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
  * Created by angel
  */
object ImproveData extends ProcessData{
  val DATA_PATH = GlobalConfigUtils.getDataPath
  val TO_TABLENAME = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster

  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    val jsondata: DataFrame = sQLContext.read.format("json").load(DATA_PATH)

    //TODO 1 : 根据IP解析出经纬度和省 - 市
    //获取IP
    val rdd: RDD[Row] = jsondata.select("ip").rdd
    val ipstr: RDD[String] = rdd.map {
      line => line.getAs[String]("ip")
    }
    //把所有的IP拿出来，封装到一个List集合里面
    val ipList: List[String] = ipstr.collect().toBuffer.toList
    //ipList: List[String] ---> Seq(La_lo(ip , la , lo , region , city))
    val parse2bean: Seq[La_lo_region_city] = ParseIp2Bean.parse2bean(ipList)
    //--->paralize(Seq())-->rdd
    val rddBean: RDD[La_lo_region_city] = sparkContext.parallelize(parse2bean)
    //--->DF--->kudu
    import sQLContext.implicits._
    val dF = rddBean.toDF
    //IP|经度|纬度|省份|城市
    jsondata.registerTempTable("ods")
    dF.registerTempTable("La_lo_region_city")
    //表关联
    val result: DataFrame = sQLContext.sql(ContantsSQL.odssql)
    //定义schema ， 在kudu上构建ODS
    val schema:Schema = ContantsSchema.odsSchema
    val partitionID = "ip"
//    //TODO 2: 将数据落地--kudu --ODS
    DBUtils.process(kuduContext , result , TO_TABLENAME , KUDU_MASTER , schema , partitionID)

  }
}
