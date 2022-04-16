package com.dmp.tags

import java.util

import com.dmp.Graphx.Graphx
import com.dmp.`trait`.ProcessData
import com.dmp.aggregateTag.Tags_agg
import com.dmp.tags.mkTags._
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.kudu.spark.kudu._
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
/**
  * Created by angel
  */
object DataTags extends ProcessData{
  /*
  1: 提前过滤掉不符合规范的数据集
  2: 生成当天的标签(性别、年龄、商圈、广告位类型、APP名称、渠道、设备（WIFI、IOS、运营商）、关键词、地域标签、用户的识别码)
  3: 统一用户识别
  4: 标签聚合
  5: 标签回溯（当天标签和历史表亲--标签衰减 进行合并）
  * */
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val TODAY_SOURCE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val kuduOptions:Map[String , String] = Map(
    "kudu.master" -> KUDU_MASTER ,
    "kudu.table" -> TODAY_SOURCE
  )
  //获取历史标签数据集
  val lastData = GlobalConfigUtils.getTagsDB + DataUtils.getYestday()
  val lastKuduOptions:Map[String , String] = Map(
    "kudu.master" -> KUDU_MASTER ,
    "kudu.table" -> lastData
  )

  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //XRX100003##YY直播
    //1):加载字典文件
    val appIdName = sparkContext.textFile(GlobalConfigUtils.appIdName)
    val deviceDic = sparkContext.textFile(GlobalConfigUtils.deviceDic)
    //2):处理字典文件
    val app: Map[String, String] = appIdName.map {
      var map = Map[String, String]()
      line =>
        val arr = line.split("##")
        map += (arr(0) -> arr(1))
        map
    }.collect().flatten.toMap
    val device: Map[String, String] = deviceDic.map {
      var map = Map[String, String]()
      line =>
        val arr = line.split("##")
        map += (arr(0) -> arr(1))
        map
    }.collect().flatten.toMap
    //3):广波字典文件
    val Bapp = sparkContext.broadcast(app)
    val Bdevice = sparkContext.broadcast(device)
    //1: 提前过滤掉不符合规范的数据集
    val data = sQLContext.read.options(kuduOptions).kudu
    val ods: Dataset[Row] = data.where(ContantsSQL.non_empty_UID)
    //2: 生成当天的标签
    val odsRdd: RDD[Row] = ods.rdd
    val tags: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = odsRdd.map {
      line =>
        //1:生成广告位类型标签
        val adType = Tags_adType.makeTags(line)
        //2:APP名称
        val appname = Tags_app.makeTags(line, Bapp.value)
        //3:渠道标签
        val channel = Tags_Channel.makeTags(line)
        //4:设备标签
        val device = Tags_device.makeTags(line, Bdevice.value)
        //5:关键词标签
        val keywords = Tags_keywords.makeTags(line)
        //6:地域标签
        val area = Tags_Area.makeTags(line)
        //7：性别标签
        val sex = Tags_Sex.makeTags(line)
        //8: 年龄标签
        val age = Tags_age.makeTags(line)
        //9:商圈标签
        val trade = Tags_trade.makeTags(line)
        val tags = adType ++ appname ++ channel ++ device ++ keywords ++ area ++ sex ++ age ++ trade
        //10:获取用户的唯一标识
        val listIds: util.LinkedList[String] = DataUtils.getUserId(line)
        val uid = listIds.getFirst.toString
        var map = Map[String, Int]()
        for (index <- 0 until listIds.size()) {
          map += (listIds.get(index) -> 0)
        }

        //TODO -------------
        (uid, (map.toList, tags.toList))
    }
//    tags.saveAsTextFile("/Users/angel/Desktop/aa")
    //统一用户识别
    /**
      * userid=1 , tags1
      * userid=1 , tags2
      *
      * ===>
      * userid=1 , tag
      * */
    val graph: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = Graphx.graph(tags , odsRdd)
    //标签聚合操作
    val todayData: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = Tags_agg.agg(graph)
    val today: RDD[(String, String)] = todayData.map {
      line =>
        (line._2._1.mkString, line._2._2.mkString)
    }

    //将当天标签数据集的格式处理
    import sQLContext.implicits._
    val todayDF: DataFrame = today.toDF("userids" , "tags")
    val todayRDD: RDD[Row] = todayDF.rdd

//    resultData.saveAsTextFile("/Users/angel/Desktop/result")

    /**
      * 问题：
      * 1：只生成了当天的标签数据集
      * 2：如何进行标签合并：当天+历史
      * 3：合并期间的问题：
      * 3.1：历史标签数据集的权重：标签衰减 ---》当天+历史（衰减后）
      *
      * imid = 1 张三  userid=1 , tags1
      * userid=1 , tags2
      * imid = 1 李四  userid=2 , tags3
      *
      * 3.2：合并数据集之后，可能出现用户的识别码（当天和历史中有重合的userid）
      * 今天的标签（appname:抖音 ， keywords:天文(2.2)、地雷(2.3)、娱乐(3.2)）
      * 历史的标签 （appname：皮皮虾 ， keyswords：spark\hive\娱乐(5,4)）
      * ==> (appname:抖音（3.2） , 皮皮虾（3.3） ， keywords：天文(2.2)、地雷(2.3)、娱乐(3.2)、spark\hive\娱乐(5,4))
      * 3.3：当天：（K美文 , 3）   (K美文 ， 6)
      * */

    /**
      * 解决：
      * 标签衰减：牛顿冷却系数数学模型 ： F(t) = 初始温度*expt(-冷却系数*时间间隔)
      * 最终标签权重 = 衰减系数*行为权重 + 当前权重
      * 衰减系数：产品---》数字
      * 行为权重：历史标签数据集中的权重
      * 当前权重：当天标签数据集的权重
      *
      * 3.2：
      * 在进行统一用户识别
      * 3.3：
      * 在进行标签聚合操作
      * */

    //获取历史标签数据集
    val lastData: DataFrame = sQLContext.read.options(lastKuduOptions).kudu
    val lastDataRDD: RDD[Row] = lastData.rdd
    //合并标签操作
    Merge_Tags.merge(todayRDD , lastDataRDD , odsRdd , sQLContext , kuduContext)

    /**
      * 一下操作不是本项目常规内容，是为了生成历史标签数据集
      * */

//    val schema:Schema = ContantsSchema.tags
//    val partitionID = "userids"
//    DBUtils.process(kuduContext , todayDF , lastData , KUDU_MASTER , schema , partitionID)














  }
}
