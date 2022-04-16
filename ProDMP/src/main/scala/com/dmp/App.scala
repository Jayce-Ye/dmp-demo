package com.dmp

import com.dmp.ETL._
import com.dmp.tags.DataTags
import com.dmp.toEs.TagsSinkEs
import com.dmp.tools.GlobalConfigUtils
import com.dmp.trading.TradingArea
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过spark去执行不同逻辑代码
  */
object App {
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  def main(args: Array[String]): Unit = {

    @transient
    val sparkConf = new SparkConf().setAppName("APP")
      .setMaster("local[6]")
      .set("spark.worker.timeout" , GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.cores.max" , GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout" , GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.macFailures" , GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation" , GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.serializer" , GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize" , GlobalConfigUtils.sparkBuuferSize)
      .set("cluster.name" , GlobalConfigUtils.clusterName)
      .set("es.index.auto.create" , GlobalConfigUtils.autoCreateIndex)
      .set("es.nodes" , GlobalConfigUtils.esNodes)
      .set("es.port" , GlobalConfigUtils.esPort)
      .set("es.index.reads.missing.as.empty" , GlobalConfigUtils.isMissing)
      .set("es.nodes.discovery" , GlobalConfigUtils.esNodesDiscovery)
      .set("es.nodes.wan.only" , GlobalConfigUtils.wanOnly)
      .set("es.http.timeout" , GlobalConfigUtils.esTimeout)
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    val kuduContext = new KuduContext(KUDU_MASTER , sqlContext.sparkContext)
    val DATA_PATH = GlobalConfigUtils.getDataPath
//    val jsondata: DataFrame = sqlContext.read.format("json").load(DATA_PATH)
//    TODO 1）：根据IP解析出经纬度和ip所在的省--市
    ImproveData.process(sqlContext , sparkContext , kuduContext)
    //TODO 2) : 统计各地域的数量分布情况
    ProcessRegion_city.process(sqlContext , sparkContext , kuduContext)
    //TODO 3) : 广告投放的地域分布情况统计
    AdRegionAnalysis.process(sqlContext , sparkContext , kuduContext)
    //TODO 4): 广告投放的APP分布情况统计
    AppAnalysis.process(sqlContext , sparkContext , kuduContext)
    //TODO 5): 广告投放的手机设备类型分布情况统计
    DeviceAnalysis.process(sqlContext , sparkContext , kuduContext)
    //TODO 6): 广告投放的网络类型分布情况统计
    NetworkAnalysis.process(sqlContext , sparkContext , kuduContext)
    //TODO 7): 广告投放的网络运营商分布情况统计
    IspAnalysis.process(sqlContext,sparkContext , kuduContext)
    //TODO 8）: 广告投放的渠道分布情况统计
    ChannelAnalysis.process(sqlContext,sparkContext , kuduContext)

    //TODO 9):生成商圈库
    TradingArea.process(sqlContext,sparkContext , kuduContext)

//    TODO 10): 数据标签化
    DataTags.process(sqlContext,sparkContext , kuduContext)
    //TODO 11):将标签数据落地到elasticsearch
    TagsSinkEs.process(sqlContext,sparkContext , kuduContext)

    if(!sparkContext.isStopped){
      sparkContext.stop()
    }
  }

}
