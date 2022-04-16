package com.dmp.tools.ips

import java.util

import com.dmp.bean.La_lo_region_city
import com.dmp.tools.GlobalConfigUtils
import com.dmp.tools.ips.iplocation.{IPAddressUtils, IPLocation}
import com.maxmind.geoip.{Location, LookupService}

import scala.collection.JavaConverters

/**
  * Created by angel on 2018/11/8.
  */
object ParseIp2Bean {
  //根据GeoLiteCity.dat解析出IP所在的经度和维度
  //根据qqwry.dat解析出IP所在的省--市
  val GeoLiteCity = GlobalConfigUtils.getGeoLiteCity

  //定义一个方法：ipList: List[String] ---> Seq(La_lo(ip , la , lo , region , city))
  def parse2bean(ipList: List[String]):Seq[La_lo_region_city] = {
    //根据IP解析出经纬度
    val look = new LookupService(GeoLiteCity , LookupService.GEOIP_MEMORY_CACHE)
    val array = new util.ArrayList[La_lo_region_city]()
    for(ip <- ipList){
      val location: Location = look.getLocation(ip)
      val latitude: Float = location.latitude
      val longitude: Float = location.longitude
      //使用纯真数据库qqwry.dat解析出IP所在的省份和城市
      val iPAddressUtils = new IPAddressUtils()
      val locations: IPLocation = iPAddressUtils.getregion(ip)
      val region: String = locations.getRegion//省
      val city:String = locations.getCity//市
      //(ip:String , latitude:String , longtude:String , region:String , city:String)
      array.add(La_lo_region_city(ip , latitude+"" , longitude+"" , region , city))
    }
    val toSeq: Seq[La_lo_region_city] = JavaConverters.asScalaIteratorConverter(array.iterator()).asScala.toSeq
    toSeq
  }
}
