import com.dmp.tools.GlobalConfigUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by angel
  */
object findFriend {
  def main(args: Array[String]): Unit = {
    @transient
    val sparkConf = new SparkConf().setAppName("APP")
      .setMaster("local[6]")
      .set("spark.worker.timeout" , GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.cores.max" , GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout" , GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.macFailures" , GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation" , GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.serializer" , GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize" , GlobalConfigUtils.sparkBuuferSize)
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    // 图计算 --Graphx(v , e)
    //1:构建点集合
    //(userid , (name , age))
    val vertexRDD: RDD[(VertexId, (String, Int))] = sparkContext.parallelize(Seq(
      (1, ("张三", 18)),
      (2, ("李四", 19)),
      (3, ("王五", 20)),
      (4, ("赵六", 21)),
      (5, ("韩梅梅", 22)),
      (6, ("李雷", 23)),
      (7, ("小明", 24)),
      (9, ("tom", 25)),
      (10, ("jerry", 26)),
      (11, ("ession1", 27))
    ))
    //2:构建边
    val edge: RDD[Edge[Int]] = sparkContext.parallelize(Seq(
      Edge(1, 136, 0), Edge(2, 136, 0), Edge(3, 136, 0), Edge(4, 136, 0), Edge(5, 136, 0),
      Edge(4, 158, 0), Edge(5, 158, 0), Edge(6, 158, 0), Edge(7, 158, 0),
      Edge(9, 177, 0), Edge(10, 177, 0), Edge(11, 177, 0)
    ))
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD , edge)
    //点的关系
    graph.vertices.foreach(println)
    //边的关系
    graph.edges.foreach(println)
    //点和边的数量
    graph.numEdges
    graph.numVertices
    //TODO 构建连通图
    //(userid , aggid)
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
//    vertices.foreach(println)
        //1:[2：李四、王五、赵六...] 9：[tom\jerry\\\]
//    val mapdata: RDD[(VertexId, List[VertexId])] = vertices.map(line => (line._2 , List(line._1)))
//    val result: RDD[(VertexId, List[VertexId])] = mapdata.reduceByKey(_ ++ _)
//    result.foreach(println)

    //显示用户的信息
    //(userid , aggid) join (userid , (name , age)) =====> (userid , (aggid , (name , age)))
    val join: RDD[(VertexId, (VertexId, (String, Int)))] = vertices.join(vertexRDD)
    val result = join.map{
      case (userid , (aggid , (name , age))) =>
        (aggid , List((name , age)))
    }
    result.reduceByKey(_ ++ _).foreach(println)
  }
}
