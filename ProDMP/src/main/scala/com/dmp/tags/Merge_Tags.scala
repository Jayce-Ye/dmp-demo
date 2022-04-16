package com.dmp.tags

import com.dmp.Graphx.Graphx
import com.dmp.aggregateTag.Tags_agg
import com.dmp.attenu.Tags_attenu
import com.dmp.tools.{ContantsSchema, DBUtils, DataUtils, GlobalConfigUtils}
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by angel
  */
object Merge_Tags {
  val coeff = GlobalConfigUtils.coeff.toDouble
  val SINK_TABLE = GlobalConfigUtils.getTagsDB + DataUtils.NowDate()
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster


  def merge(todayData:RDD[Row] , lastData:RDD[Row] , odsRdd:RDD[Row] , sQLContext: SQLContext , kuduContext: KuduContext) = {
    //TODO 1:标签衰减
    val historyData: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = Tags_attenu.attenu(lastData , coeff)
    //TODO 2：合并 union
    val todayTags: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = operatorTodayTags(todayData)
    //TODO 标签合并（必须保证格式一致）
    val allData: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = todayTags.union(historyData)
    //TODO 3:统一的用户识别
    val graph: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = Graphx.graph(allData , odsRdd)
    //TODO 4:标签聚合
    val aggData: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = Tags_agg.agg(graph)
    val result: RDD[(String, String)] = aggData.map {
      line =>
        val uids = line._2._1.mkString
        val tags = line._2._2.mkString
        (uids, tags)
    }
    //TODO 5:落地
    import sQLContext.implicits._
    val sinkData:DataFrame = result.toDF("userids" , "tags")
    val schema:Schema = ContantsSchema.tags
    val partitionID = "userids"
    DBUtils.process(kuduContext , sinkData , SINK_TABLE , KUDU_MASTER , schema , partitionID)

  }

  def operatorTodayTags(todayData:RDD[Row]): RDD[(String, (List[(String, Int)], List[(String, Double)]))] = {
    val result = todayData.map{
      line =>
        //(MAC:FWHOXRJVLINFLCJAVFGDFQLNIXSMKTPI,0)(OPENUDID:OFJXLYNBUMFOMCUSKMDUGPUMEOCFBQKIELPBSCNG,0)
        val userids = line.getAs[String]("userids")
        //(K鸡汤,1.0)(AGE38,1.0)(D00030002,1.0)(BA东门:长途汽车站,1.0)(CZ青岛市,1.0)
        val tags = line.getAs[String]("tags")
        //K鸡汤,1.0)(AGE38,1.0)(D00030002,1.0)(BA东门:长途汽车站,1.0)(CZ青岛市,1.0
        val tmp_tags = tags.substring(1 , tags.length-1)
        //K鸡汤,1.0 AGE38,1.0 D00030002,1.0 BA东门:长途汽车站,1.0 CZ青岛市,1.0
        val tmp_arr = tmp_tags.split("\\)\\(")
        var map = Map[String , Double]()
        for(arr <- tmp_arr){
          val argArray = arr.split(",")
          val weight = argArray(1).toDouble
          val tags_name = argArray(0)
          map += (tags_name -> weight)
        }
        //处理用户的id
        //MAC:FWHOXRJVLINFLCJAVFGDFQLNIXSMKTPI,0)(OPENUDID:OFJXLYNBUMFOMCUSKMDUGPUMEOCFBQKIELPBSCNG,0
        val tmp_userid = userids.substring(1 , userids.length-1)
        //MAC:FWHOXRJVLINFLCJAVFGDFQLNIXSMKTPI,0  OPENUDID:OFJXLYNBUMFOMCUSKMDUGPUMEOCFBQKIELPBSCNG,0
        val useridArr = tmp_userid.split("\\)\\(")
        val uid = useridArr(0).split(",")(0)
        var uid_map = Map[String, Int]()
        for(arr <- useridArr){
          val uArr = arr.split(",")
          val uid_name = uArr(0)
          val uweight = uArr(1).toInt
          uid_map += (uid_name -> uweight)
        }
        (uid , (uid_map.toList , map.toList))
    }
    result
  }
}
