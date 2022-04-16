package com.dmp.Graphx

import java.util

import com.dmp.tools.DataUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * Created by angel
  */
object Graphx {

  //使用图计算进行统一用户识别
  def graph(vertex: RDD[(String, (List[(String, Int)], List[(String, Double)]))] , odsRdd: RDD[Row]) = {

    //1：构建点集合 (userid.hashcode.toLong , (用户的所有id ， tags))
    val vertextData: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = vertex.mapPartitions {
      var listBuffer = new ListBuffer[(VertexId, (List[(String, Int)], List[(String, Double)]))]
      line =>
        line.foreach {
          x =>
            listBuffer.append((x._1.toString.hashCode.toLong, x._2))
        }
        listBuffer.iterator
    }
    //2：构建边 (userid , ListBuffer[alluserid])
    val edge: RDD[Edge[Int]] = odsRdd.map {
      line =>
        val allIds: util.LinkedList[String] = DataUtils.getUserId(line)
        val userid: VertexId = allIds.getFirst.toString.hashCode.toLong
        var listBuffer = new ListBuffer[String]()
        for (index <- 0 until allIds.size()) {
          listBuffer.append(allIds.get(index))
        }
        Edge(userid, listBuffer.toString().hashCode.toLong, 0)
    }
    //3:构建图
    val graph: Graph[(List[(String, Int)], List[(String, Double)]), Int] = Graph(vertextData , edge)
    //4:构建连通图 (userid.hashCode.toLong , 顶点id)
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //5：做连通图与顶点进行join操作，获取用户的属性信息
    //(userid.hashCode.toLong , (顶点id ， （所有的id ， 标签）))
    val join: RDD[(VertexId, (VertexId, (List[(String, Int)], List[(String, Double)])))] = vertices.join(vertextData)
    val result: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = join.map {
      case (userid, (aggid, (alluserId, tags))) => (aggid, (alluserId, tags))
    }
    result
  }
}
