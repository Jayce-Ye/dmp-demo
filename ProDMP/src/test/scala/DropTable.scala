import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by angel；
  */
object DropTable {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AcctfileProcess")
      //设置Master_IP并设置spark参数
      .setMaster("local")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculationfalse", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    //使用spark创建kudu表
    val kuduContext = new KuduContext("hadoop01:7051,hadoop02:7051,hadoop03:7051", sqlContext.sparkContext)

    // TODO 指定要删除的表名称
    //    var kuduTableName = "ODS"//RegionalAnalysis  PRO_CITY    ODS  TerminalStuation
    //    val tables = Array("ODS", "ISPStuation", "PRO_CITY","channel_stuation" , "RegionalAnalysis","AppStuation")
    val tables = Array("trade")
    for(kuduTableName <- tables){
      // TODO 检查表如果存在，那么删除表
      if (kuduContext.tableExists(kuduTableName)) {
        kuduContext.deleteTable(kuduTableName)
      }
    }
  }
}
