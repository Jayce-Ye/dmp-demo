package com.dmp.tools

import java.util

import org.apache.kudu.Schema
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.kudu.spark.kudu._

/**
  * Created by angel
  */
object DBUtils {
  def process(
             kuduContext: KuduContext ,
             data:DataFrame ,
             TO_TABLENAME:String ,
             KUDU_MASTER:String ,
             schema:Schema ,
             partitionID:String
             ): Unit ={
    //TODO 1:如果表不存在，则创建表
    if(!kuduContext.tableExists(TO_TABLENAME)){
      //1）：创建kudu的客户端 ， 用来创建表的
      val kuduClient = new KuduClientBuilder(KUDU_MASTER).build()
      //2）：定义表的分区方式
      val tableOptions:CreateTableOptions = {
        //需要一个练笔爱，用来携带kudu的分区ID
        val parcols = new util.LinkedList[String]()
        parcols.add(partitionID)
        //指定kudu的分区方式
        new CreateTableOptions()
            .addHashPartitions(parcols , 6)
            .setNumReplicas(3)
      }
      //调用api，创建表
      kuduClient.createTable(TO_TABLENAME , schema , tableOptions)
    }

    //TODO 2：将数据写入kudu
    data.write
      .mode(SaveMode.Append)
      .option("kudu.table" , TO_TABLENAME)
      .option("kudu.master" , KUDU_MASTER)
      .kudu
  }
}
