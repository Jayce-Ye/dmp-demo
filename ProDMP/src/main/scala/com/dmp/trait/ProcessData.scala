package com.dmp.`trait`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by angel on 2018/11/8.
  */
trait ProcessData {
  def process(sQLContext: SQLContext , sparkContext: SparkContext , kuduContext: KuduContext)
}
