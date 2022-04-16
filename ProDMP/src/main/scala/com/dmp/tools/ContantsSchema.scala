package com.dmp.tools

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import scala.collection.JavaConverters._
/**
  * Created by angel
  */
object ContantsSchema {
  //1：初始化
  lazy val odsSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ip", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("sessionid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("advertisersid",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adorderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("requestmode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("requestdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("client", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("osversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("density", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("pw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ph", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("long", Type.STRING).nullable(false).build(),//TODO
      new ColumnSchemaBuilder("lat", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("ispid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iseffective", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbilling", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("devicetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("processnode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("apptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("district", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("paymode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("iswin", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("cur", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("imei", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mac", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfa", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androidid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("storeurl", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("realip", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("aw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ah", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("userid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("title", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("keywords", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tagid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mediatype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("email", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tel", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("sex", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("age", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //2：统计地域分布数量
  lazy val ProcessRegionCity: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("cityname" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("NUM" , Type.INT64).nullable(false).key(true).build()
    ).asJava
    new Schema(columns)
  }



  //3: 广告投放的地域分布情况
  lazy val AdRegionAnalysis:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("cityname" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("OriginalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("ValidRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidSus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsSusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClickRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }

  //4: 广告投放的APP分布情况
  lazy val APPAnalysis:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("appid" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("appname" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("OriginalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("ValidRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidsSus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsSusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClickRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }

  //5: 广告投放的设备分布情况
  lazy val deviceAnalysis:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("client" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("device" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("OriginalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("ValidRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidsSus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsSusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClickRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }

  //6: 广告投放的网络类型分布情况
  lazy val networkAnalysis:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("networkmannerid" , Type.INT64).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("networkmannername" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("OriginalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("ValidRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidsSus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsSusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClickRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }

  //7: 广告投放的网络运营商分布情况
  lazy val IspAnalysis:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ispname" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("OriginalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("ValidRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidsSus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsSusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClickRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }
  //8: 广告投放的渠道分布情况
  lazy val ChannelAnalysis:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("channelid" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("OriginalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("ValidRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidsSus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsSusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClickRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }

  //9:生成商圈库
  lazy val trade:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("geoHashCode" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("trade" , Type.STRING).nullable(false).build()  //原始请求数
    ).asJava
    new Schema(columns)
  }

  //10:生成标签库
  lazy val tags:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("userids" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("tags" , Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }





}
