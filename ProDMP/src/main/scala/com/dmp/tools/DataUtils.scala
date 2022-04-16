package com.dmp.tools

import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.zip.CRC32
import java.util.{Calendar, Date, UUID}

import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object DataUtils {

  /*
  * 获取当天的时间
  * @return yyyyMMdd
  * */
  def NowDate(): String = {
    val now = new Date()
    //TODO SimpleDateFormat 线程不安全的实现20181010  2018101
    //    val simpleDateFormat:SimpleDateFormat
    val fastDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val formatTime = fastDateFormat.format(now)
    //yyyy-MM-dd HH:mm:ss ----> yyyyMMdd
    val date: String = fmtDate(formatTime).getOrElse("sorry , no time")
    date
  }

  /**
    * 获取当天昨天日期
    *
    * */
  def getYestday():String = {
    val dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val instance: Calendar = Calendar.getInstance()
    instance.add(Calendar.DATE , -1)
    val yestDay = dateFormat.format(instance.getTime)
    yestDay
  }





  /**
    * 格式化日期
    *
    * @param formatTime
    * @return yyyyMMdd
    **/
  def fmtDate(formatTime: String): Option[String] = {
    try {
      if (StringUtils.isNotEmpty(formatTime)) {
        val fields: Array[String] = formatTime.split(" ")
        if (fields.length > 1) {
          Some(fields(0).replace("-", ""))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  /**
    * 获取用户的唯一标识
    * */
  def getUserId(row: Row):util.LinkedList[String] = {
    var list = new util.LinkedList[String]()
    //获取手机串号
    if(row.getAs[String]("imei").nonEmpty){
      list.add("IMEI:"+row.getAs[String]("imei").toUpperCase)
    }
    if(row.getAs[String]("imeimd5").nonEmpty){
      list.add("IMEIMD5:"+row.getAs[String]("imeimd5").toUpperCase)
    }
    if(row.getAs[String]("imeisha1").nonEmpty){
      list.add("IMEISHA1:"+row.getAs[String]("imeisha1").toUpperCase)
    }
    //获取手机mac码
    if(row.getAs[String]("mac").nonEmpty){
      list.add("MAC:"+row.getAs[String]("mac").toUpperCase)
    }
    if(row.getAs[String]("macmd5").nonEmpty){
      list.add("MACMD5:"+row.getAs[String]("macmd5").toUpperCase)
    }
    if(row.getAs[String]("macsha1").nonEmpty){
      list.add("MACSHA1:"+row.getAs[String]("macsha1").toUpperCase)
    }
    //获取手机app的广告码
    if(row.getAs[String]("idfa").nonEmpty){
      list.add("IDFA:"+row.getAs[String]("idfa").toUpperCase)
    }
    if(row.getAs[String]("idfamd5").nonEmpty){
      list.add("IDFAMD5:"+row.getAs[String]("idfamd5").toUpperCase)
    }
    if(row.getAs[String]("idfasha1").nonEmpty){
      list.add("IDFASHA1:"+row.getAs[String]("idfasha1").toUpperCase)
    }
    //获取苹果设备的识别码
    if(row.getAs[String]("openudid").nonEmpty){
      list.add("OPENUDID:"+row.getAs[String]("openudid").toUpperCase)
    }
    if(row.getAs[String]("openudidmd5").nonEmpty){
      list.add("OPENUDIDMD5:"+row.getAs[String]("openudiddm5").toUpperCase)
    }
    if(row.getAs[String]("openudidsha1").nonEmpty){
      list.add("OPENUDIDSHA1:"+row.getAs[String]("openudidsha1").toUpperCase)
    }
    //获取安卓设备的识别码
    if(row.getAs[String]("androidid").nonEmpty){
      list.add("ANDROIDID:"+row.getAs[String]("androidid").toUpperCase)
    }
    if(row.getAs[String]("androididmd5").nonEmpty){
      list.add("ANDROIDIDMD5:"+row.getAs[String]("androididmd5").toUpperCase)
    }
    if(row.getAs[String]("androididsha1").nonEmpty){
      list.add("ANDROIDIDSHA1:"+row.getAs[String]("androididsha1").toUpperCase)
    }

    list
  }
}