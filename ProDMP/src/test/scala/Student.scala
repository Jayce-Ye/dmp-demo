import com.dmp.tools.GlobalConfigUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by angel
  */
object Student {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("APP")
      .setMaster("local[6]")
      .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.cores.max", GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout", GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.macFailures", GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation", GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.serializer", GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize", GlobalConfigUtils.sparkBuuferSize)
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sQLContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    val source: RDD[String] = sparkContext.textFile("/Users/angel/Desktop/raw_data.txt")

    val mapData = source.flatMap {
      line =>
        val arrs: Array[String] = line.split("/n")
        arrs
    }
    val data:RDD[DataX] = mapData.map {
      line =>
        val arr = line.split("\\s+")
        val d = line match {
          case line if (line.startsWith("com")) => doData1(arr)
          case _ => DataX("BLANK",null)
        }
        d
    }.filter(line => !line.data.equals("BLANK"))

    val infoData:RDD[loginfo] = mapData.map {
      line =>
        val arr = line.split("\\s+")
        val x = line match {
          case line if (isStartWithNum(line)) => doData2(arr)
          case _ => loginfo("BLANK" , null , null ,null,null)
        }
        x
    }.filter(line => !line.nginxTime.equals("BLANK"))

    infoData.foreach(println)


//    data.map{
//      line =>
//        val package_name = line.package_name
//        val str = line.data
//        val linfo = str.substring(1 , str.length-1)
//        package_name match{
//          case package_name if(package_name.endsWith("thirdAppPlayBehavior")) =>
//          case package_name if(package_name.endsWith("com.ktcp.video")) =>
//
//          case _ => null//还有更多种
//        }
//
////        val arr = linfo.split(",")
////        for(x <- arr){
////          val split: Array[String] = x.split("=")
////          val length = split.length
////          val o1 = if(length>0) split(0) else ""
////          val o2 = if(length>1) split(1) else ""
////          val o3 = if(length>2) split(2) else ""
////          val o4 = if(length>3) split(3) else ""
////          val o5 = if(length>4) split(4) else ""
////          val o6 = if(length>5) split(5) else ""
////          val o7 = if(length>6) split(6) else ""
////
////
////        }
//    }






    }



  def isStartWithNum(str: String) = {
    val regex ="""^\d+?.*$""".r
    regex.findFirstIn(str) != None
  }

    def isIntByRegex(s : String) = {
      val pattern = """^\d+$""".r
      s match {
        case pattern(_*) => true
        case _ => false
      }
    }


  def doData1(arr:Array[String]):DataX = {
    val length = arr.length
    val packageName = if(length > 0) arr(0) else ""
    val data = if(length > 1) arr(1) else ""
    DataX(packageName , data)
  }

  def doData2(arr:Array[String]): loginfo = {
    val length = arr.length
    val nginxTime = if (length > 1) arr(0) + " " + arr(1) else ""
    val useAgent = if (length > 2) arr(2) else ""
    val logLevel = if (length > 3) arr(3) else ""
    val dataLog = if (length > 4) arr(4) else ""
    val data = if (length > 5) arr(5) else ""

    //        val ip = if(length > 5) arr(5) else ""
    //        val source = if(length > 6) arr(6) else ""
    //        val deviceid = if(length>7) arr(7) else ""
    //        val clienttype = if(length>8) arr(8) else ""
    //        val version = if(length>9) arr(9) else ""
    //        val data = if(length>10) arr(10) else ""
    loginfo(
      nginxTime,
      useAgent,
      logLevel,
      dataLog,
      data
    )
  }

}


case class DataX(
                    package_name:String ,
                    data:String
                  )

case class loginfo(
                  nginxTime:String ,
                  useAgent:String ,
                  logLevel:String ,
                  dataLog:String ,
                  data:String
//                  ip:String ,
//                  source:String ,
//                  deviceid:String ,
//                  clienttype:String ,
//                  version:String ,
//                  data:String
                  )

//client_type,mac_line,package_name,start_time,end_time,program_name,duration,origin,version,total_duration,dt
class LOG {
  private var client_type: String = null
  private var mac_line: String = null
  private var package_name: String = null
  private var start_time: String = null
  private var end_time: String = null
  private var program_name: String = null
  private var duration: String = null
  private var origin: String = null
  private var version: String = null
  private var total_duration:String = null
  private var dt: String = null

  def setClient_type(value:String) = this.client_type=value
  def setmac_line(value:String) = this.mac_line=value
  def setpackage_name(value:String) = this.package_name=value
  def setstart_time(value:String) = this.start_time=value
  def setend_time(value:String) = this.end_time=value
  def setprogram_name(value:String) = this.program_name=value
  def setduration(value:String) = this.duration=value
  def setorigin(value:String) = this.origin=value
  def setversion(value:String) = this.version=value
  def settotal_duration(value:String) = this.total_duration=value
  def setdt(value:String) = this.dt=value
}
