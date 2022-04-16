package com.dmp.toEs

import com.dmp.`trait`.ProcessData
import com.dmp.bean.Tags
import com.dmp.tools.{DataUtils, GlobalConfigUtils}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
/**
  * Created by angel.
  */
object TagsSinkEs extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE = GlobalConfigUtils.getTagsDB + DataUtils.NowDate()
  val kuduOptions:Map[String , String] = Map(
    "kudu.master" ->KUDU_MASTER ,
    "kudu.table" -> SOURCE
  )
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {

    //1:获取标签数据
    val sourceTable: DataFrame = sQLContext.read.options(kuduOptions).kudu
    val rdd: RDD[Row] = sourceTable.rdd
    //2:处理标签数据
    val es: RDD[(String, Map[String, String])] = rdd.map {
      line =>
        val userids = line.getAs[String]("userids")
        //(K财经,1.92)(CN123486,1.92)(CZ吉林省,1.92)
        val tags = line.getAs[String]("tags")
        //K财经,1.92)(CN123486,1.92)(CZ吉林省,1.92
        val tmp_tags = tags.substring(1, tags.length - 1)
        //K财经,1.92 CN123486,1.92 CZ吉林省,1.92
        val tagsArr = tmp_tags.split("\\)\\(")
        var obj = new Tags
        for (arr <- tagsArr) {
          val arrSplit = arr.split(",")
          val k = arrSplit(0)
          val v = arrSplit(1)
          //ID , (K,V)
          //使用面向对象的思想处理：将k，v封装到对象里面去
          /*
          1##D00010001
          2##D00010002
          3##D00010003
          4##D00010004
          WIFI##D00020001
          4G##D00020002
          3G##D00020003
          2G##D00020004
          NETWORKOTHER##D00020005
          移动##D00030001
          联通##D00030002
          电信##D00030003
          OPERATOROTHER##D00030004
          * */
          k match {
            //os
            case k if (k.startsWith("D00010001")) => obj.setOs(("android", v))
            case k if (k.startsWith("D00010002")) => obj.setOs(("ios", v))
            case k if (k.startsWith("D00010003")) => obj.setOs(("wp", v))
            case k if (k.startsWith("D00010004")) => obj.setOs(("其他", v))
            //NETWORK
            case k if (k.startsWith("D00020001")) => obj.setNetwork(("WIFI", v))
            case k if (k.startsWith("D00020002")) => obj.setNetwork(("4G", v))
            case k if (k.startsWith("D00020003")) => obj.setNetwork(("3G", v))
            case k if (k.startsWith("D00020004")) => obj.setNetwork(("2G", v))
            case k if (k.startsWith("D00020005")) => obj.setNetwork(("NETWORKOTHER", v))
            //ISP
            case k if (k.startsWith("D00030001")) => obj.setIsp(("移动", v))
            case k if (k.startsWith("D00030002")) => obj.setIsp(("联通", v))
            case k if (k.startsWith("D00030003")) => obj.setIsp(("电信", v))
            case k if (k.startsWith("D00030004")) => obj.setIsp(("OPERATOROTHER", v))
            //APP
            case k if (k.startsWith("APP")) => obj.setApp((k.substring(3, k.length), v))
            //PZ
            case k if (k.startsWith("PZ")) => obj.setPz((k.substring(2, k.length), v))
            //CZ
            case k if (k.startsWith("CZ")) => obj.setCz((k.substring(2, k.length), v))
            //LC
            case k if (k.startsWith("LC")) => obj.setLc((k.substring(2, k.length), v))
            //CN
            case k if (k.startsWith("CN")) => obj.setCn((k.substring(2, k.length), v))
            //BA
            case k if (k.startsWith("BA")) => obj.setBa((k.substring(2, k.length), v))
            //keywords
            case k if (k.startsWith("K")) => obj.setKeywords((k.substring(1, k.length), v))
            //sex
            case k if (k.startsWith("SEX")) => obj.setSex((k.substring(3, k.length), v))
            //age
            case k if (k.startsWith("AGE")) => obj.setAge((k.substring(3, k.length), v))
          }
        }
        //(ANDROIDID:YDUQUJXNFIHIXZTJ,0)(OPENUDID:KCRHTGFQEQLNEWCTPRYRQTZKUHKGDVKYNVKOFMKB,0)(
        val id = userids.substring(1, userids.length - 1).split("\\)\\(")(0).split(",")(0)
        (id, obj.toData)
    }

    //3:将处理完的数据落地到es
    import org.elasticsearch.spark._
    es.saveToEsWithMeta("tags/doc")

  }
}
