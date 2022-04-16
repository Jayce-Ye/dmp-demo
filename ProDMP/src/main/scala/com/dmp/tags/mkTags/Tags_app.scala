package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_app extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {
    var map =  Map[String, Double]()
    if(args.length > 1){
      val row = args(0).asInstanceOf[Row]
      val brodcast_value = args(1).asInstanceOf[Map[String, String]]
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      //获取app名称的函数
      val readAppName:Option[String] => String = {
        case Some(x) => x
        case None => brodcast_value.getOrElse(appid , appname)
      }
      val appName = readAppName(Some(appname))
      if(StringUtils.isNotBlank(appName) && !"".equals(appName)){
        map += ("APP"+appName -> 1)
      }
    }
    map
  }
}
