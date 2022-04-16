package com.dmp.tools

import com.typesafe.config.ConfigFactory

/**
  * Created by angel on 2018/11/7.
  */
class GlobalConfigUtils {
  def conf = ConfigFactory.load()
  //开始加载spark相关的配置参数
  def sparkWorkTimeout = conf.getString("spark.worker.timeout")
  def sparkRpcTimeout = conf.getString("spark.rpc.askTimeout")
  def sparkNetWorkTimeout = conf.getString("spark.network.timeoout")
  def sparkMaxCores = conf.getString("spark.cores.max")
  def sparkTaskMaxFailures = conf.getString("spark.task.maxFailures")
  def sparkSpeculation = conf.getString("spark.speculation")
  def sparkAllowMutilpleContext = conf.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer = conf.getString("spark.serializer")
  def sparkBuuferSize = conf.getString("spark.buffer.pageSize")
  //开始加载es相关配置参数
  def clusterName = conf.getString("cluster.name")
  def autoCreateIndex = conf.getString("es.index.auto.create")
  def esNodes = conf.getString("esNodes")
  def esPort = conf.getString("es.port")
  def isMissing = conf.getString("es.index.reads.missing.as.empty")
  def esNodesDiscovery = conf.getString("es.nodes.discovery")
  def wanOnly = conf.getString("es.nodes.wan.only")
  def esTimeout = conf.getString("es.http.timeout")
  //获取kudu的相关配置参数
  def kuduMaster = conf.getString("kudu.master")
  //获取数据源
  def getDataPath = conf.getString("data.path")
  //获取解析IP相关的参数
  def getGeoLiteCity = conf.getString("GeoLiteCity.dat")
  def IP_FILE = conf.getString("IP_FILE")
  def INSTALL_DIR = conf.getString("INSTALL_DIR")
  //获取ODS前缀
  def ODS_PREFIX = conf.getString("DB.ODS.PREFIX")
  //统计地域数量分布情况
  def ProcessRegionCity = conf.getString("ProcessRegionCity")
  //统计广告地域分布情况
  def AdRegionAnalysis = conf.getString("AdRegionAnalysis")
  //统计广告的APP分布情况
  def AppAnalysis = conf.getString("AppAnalysis")
  //设备分布情况
  def DeviceAnalysis = conf.getString("DeviceAnalysis")
  //网络类型分布情况
  def NetworkAnalysis = conf.getString("NetworkAnalysis")
  //运营商
  def IspAnalysis = conf.getString("IspAnalysis")
  //渠道
  def ChannelAnalysis = conf.getString("ChannelAnalysis")
  //高德key
  def getKey = conf.getString("key")
  //商圈库
  def tradeDB = conf.getString("tradeDB")
  //加载字典文件
  def appIdName = conf.getString("appIdName")
  def deviceDic = conf.getString("deviceDic")
  //impala的配置参数
  def JDBC_DRIVER = conf.getString("JDBC_DRIVER")
  def CONNECTION_URL = conf.getString("CONNECTION_URL")
  //获取历史标签数据集
  def getTagsDB = conf.getString("DB.TAG.PREFIX")
  //标签衰减系数
  def coeff = conf.getString("coeff")

}

object GlobalConfigUtils extends GlobalConfigUtils
