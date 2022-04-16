package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_device extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {

    var map = Map[String , Double]()
    if(args.length > 1){
      val row = args(0).asInstanceOf[Row]
      val brodcastValue = args(1).asInstanceOf[Map[String , String]]
      //client
      val client = row.getAs[Long]("client").toInt
      //networkmannername
      val networkmannername = row.getAs[String]("networkmannername")
      //ispname
      val ispname = row.getAs[String]("ispname")

      /**
      1##D00010001
      2##D00010002
      3##D00010003
      4##D00010004
        * */
      val os = brodcastValue.getOrElse(client.toString.toUpperCase , brodcastValue.get("4").get)
      /*
      WIFI##D00020001
      4G##D00020002
      3G##D00020003
      2G##D00020004
      NETWORKOTHER##D00020005
      * */
      val network = brodcastValue.getOrElse(networkmannername.toString.toUpperCase , brodcastValue.get("NETWORKOTHER").get)
      /**
      移动##D00030001
      联通##D00030002
      电信##D00030003
      OPERATOROTHER##D00030004
        * */
      val isp = brodcastValue.getOrElse(ispname.toString.toUpperCase , brodcastValue.get("OPERATOROTHER").get)
      map += (os -> 1)
      map += (network -> 1)
      map += (isp -> 1)

    }
    map
  }
}
