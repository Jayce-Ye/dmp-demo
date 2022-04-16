package com.dmp.trading

import java.util

import com.dmp.tools.{GlobalConfigUtils, ParseJson}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

/**
  * Created by angel
  */
object ExtractTrading {

  def getArea(location:String):String = {
    //1：构建HTTPclient的客户端
    //https://restapi.amap.com/v3/geocode/regeo?&location=116.310003,39.991957&key=6d5bd7e435a61f48ebc572e4f4945799
    //代码发送-->url -->response -->商圈
    val client = new HttpClient
    //2：封装请求的URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?&location="+location+"&key="+GlobalConfigUtils.getKey
    //3：提交请求
    val method = new GetMethod(url)
    val code: Int = client.executeMethod(method)
    var temp = ""
    if(code == 200){
      val response = method.getResponseBodyAsString
      val trade: String = ParseJson.parse(response)
      temp = trade
    }
    temp
  }
}
