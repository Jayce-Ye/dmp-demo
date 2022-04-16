package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_Sex extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String , Double]()
    if(args.length > 0){
      val row = args(0).asInstanceOf[Row]
      val sex = row.getAs[String]("sex")
      val field = sex match{
        case "0" => "男"
        case _ => "女"
      }
      map += ("SEX:"+field -> 1)
    }
    map
  }
}
