/**
  * Created by angel
  */
import java.io.FileWriter

import com.google.gson.Gson
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.Map
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SpiderLauncher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SpiderLauncher")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val inputStream = ssc.socketTextStream("localhost" , 9999)
    val map: DStream[(String, Int)] = inputStream.map(line => (line, 1))
    var ipBlockAccessCountsMap = scala.collection.Map[String, Int]()
    map.foreachRDD{
     line =>
           ipBlockAccessCountsMap = line.collectAsMap()
       println("==========================="+ipBlockAccessCountsMap)
   }
    ssc.start()
    ssc.awaitTermination()
  }
}