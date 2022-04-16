package com.dmp.trading

import ch.hsr.geohash.GeoHash
import com.dmp.`trait`.ProcessData
import com.dmp.bean.BArea
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
/**
  * Created by angel
  */
object TradingArea extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.tradeDB
  val kuduOptions:Map[String , String] = Map(
    "kudu.master" -> KUDU_MASTER ,
    "kudu.table" -> SOURCE_TABLE
  )
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //1: 获取数据IP所在的经纬度
    val data = sQLContext.read.options(kuduOptions).kudu
    //TODO 过滤非中国地域 73<long<136  3<lat<54
    data.registerTempTable("ods")
    val long_lat: DataFrame = sQLContext.sql(ContantsSQL.filer_non_china)
    val rdd = long_lat.rdd
    val trade: RDD[BArea] = rdd.map {
      line =>
        val long = line.getAs[String]("long")
        //经度
        val lat = line.getAs[String]("lat")
        //纬度
        val geoHashCode = GeoHash.withCharacterPrecision(lat.toDouble, long.toDouble, 8).toBase32
        //根据经纬度生成GeoHash编码
        //2：生成商圈信息
        val location = long + "," + lat
        val trade: String = ExtractTrading.getArea(location)
        BArea(geoHashCode, trade)

    }.filter { line => !line.trade.equals("blank") }

    //3：数据落地：生成自己的商圈库
    import sQLContext.implicits._
    val result = trade.toDF("geoHashCode" , "trade")
    val schema:Schema = ContantsSchema.trade
    val partitionID = "geoHashCode"
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)


  }
}
