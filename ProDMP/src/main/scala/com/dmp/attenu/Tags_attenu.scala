package com.dmp.attenu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_attenu {
  /**
    * 标签衰减工作
    * */
  def attenu(lastData:RDD[Row] , coeff:Double):RDD[(String, (List[(String, Int)], List[(String, Double)]))] = {
    val result = lastData.map{
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
          val weight = argArray(1).toDouble * coeff
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
