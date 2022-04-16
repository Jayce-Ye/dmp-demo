package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_keywords extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String , Double]()
    //农村,农民,手工艺,农业,酱油
    if(args.length > 0){
      val row = args(0).asInstanceOf[Row]
      val keywords = row.getAs[String]("keywords")
      if(StringUtils.isNotBlank(keywords)){
        val fields = keywords.split(",")
        fields.map{
          line =>
            map += ("K".concat(line) -> 1)
        }
      }
    }
    map
  }
}
