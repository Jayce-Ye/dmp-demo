package com.dmp.tags.mkTags

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import ch.hsr.geohash.GeoHash
import com.dmp.`trait`.Tags
import com.dmp.tools.GlobalConfigUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_trade extends Tags{
  val JDBC_DRIVER = GlobalConfigUtils.JDBC_DRIVER
  val CONNECTION_URL = GlobalConfigUtils.CONNECTION_URL

  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {
    //1:sparkSql --> kudu---->action--(慢)
    //2：scala --》impala-->kudu
    var map = Map[String , Double]()
    if(args.length > 0){
      val row = args(0).asInstanceOf[Row]
      //获取经纬度
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      //73<long<136  3<lat<54
      if(long > 73 && long < 136 && lat > 3 && lat < 54){
        val geoHashCode = GeoHash.withCharacterPrecision(lat, long, 8).toBase32
        val sql = "select location from trade where geohashcode='"+geoHashCode+"'"
        Class.forName(JDBC_DRIVER)
        val con = DriverManager.getConnection(CONNECTION_URL)
        try{
          if(StringUtils.isNotBlank(geoHashCode)){
            //TYPE_FORWARD_ONLY:只允许结果集的游标向下移动
            //CONCUR_READ_ONLY:不能用结果集更新数据库中的表
            val statement = con.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
            val rs = statement.executeQuery(sql)
            while (rs.next){
              val trade = rs.getString("location")
              map += ("BA"+trade -> 1)
            }
          }
        }catch {
          case e:Exception => e.printStackTrace()
        }finally {
          con.close()
        }

      }
    }
    map
  }
}
