package com.dmp.aggregateTag

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
  * Created by angel
  */
object Tags_agg {
  /**
    * 标签聚合操作
    * @param graph
    * */
  def agg(graph: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))]):RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = {
    val resultData: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = graph.reduceByKey {
      case (before, after) =>
        val uid = before._1 ++ after._1
        val tags = before._2 ++ after._2
        //将标签出现重复的现象进行权重的叠加
        val groupTags = tags.groupBy(line => line._1)
        val resultTags = groupTags.mapValues {
          line =>
            line.foldLeft(0.0)((k, v) => k + v._2)
        }.toList
        val distinctID = uid.distinct
        (distinctID, resultTags)

    }
    resultData
  }
}
