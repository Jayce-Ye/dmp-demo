package com.dmp.bean

import scala.collection.mutable.ListBuffer

/**
  * Created by angel
  */
class Tags {
  private var os:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var network:ListBuffer[(String , String)] = new ListBuffer[(String , String)]()
  private var isp:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var app:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var pz:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var cz:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var lc:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var cn:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var ba:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var keywords:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var sex:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()
  private var age:ListBuffer[(String , String)] = new ListBuffer[(String, String)]()

  //set方法
  def setOs(os:(String , String)) = {this.os.append(os)}
  def setNetwork(network:(String , String)) = {this.network.append(network)}
  def setIsp(isp:(String , String)) = {this.isp.append(isp)}
  def setApp(app:(String , String)) = {this.app.append(app)}
  def setPz(pz:(String , String)) = {this.pz.append(pz)}
  def setCz(cz:(String , String)) = {this.cz.append(cz)}
  def setLc(lc:(String , String)) = {this.lc.append(lc)}
  def setCn(cn:(String , String)) = {this.cn.append(cn)}
  def setBa(ba:(String , String)) = {this.ba.append(ba)}
  def setKeywords(keywords:(String , String)) = {this.keywords.append(keywords)}
  def setSex(sex:(String , String)) = {this.sex.append(sex)}
  def setAge(age:(String , String)) = {this.age.append(age)}

  //toString
  def toData:Map[String , String] = Map(
    "os" -> os.mkString ,
    "network" -> network.mkString ,
    "isp" -> isp.mkString ,
    "app" -> app.mkString ,
    "pz" -> pz.mkString ,
    "cz" -> cz.mkString ,
    "lc" -> lc.mkString ,
    "cn" -> cn.mkString ,
    "ba" -> ba.mkString ,
    "keywords" -> keywords.mkString ,
    "sex" -> sex.mkString ,
    "age" -> age.mkString
  )
}
