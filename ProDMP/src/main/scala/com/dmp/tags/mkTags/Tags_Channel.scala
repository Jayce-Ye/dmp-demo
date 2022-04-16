package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_Channel extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {
    var map =  Map[String, Double]()
    if(args.length > 0){
      val row = args(0).asInstanceOf[Row]
      val channelid = row.getAs[String]("channelid")
      if(StringUtils.isNotBlank(channelid)){
        map += ("CN"+channelid -> 1)
      }
    }
    map
  }
}
