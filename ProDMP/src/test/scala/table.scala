import com.dmp.tools.GlobalConfigUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by angel
  */
object table {
  def main(args: Array[String]): Unit = {
    val str = "{1=com.tcl.tv,2=11.78M,3=0.02%,4=2019-01-31,23:00:47}"
//    println(str.substring(1 , str.length-1))


    println(addInt(1,1))

  }

  def demo(x:Int) = {}
  def isStartWithNum(str: String) = {
    val regex ="""^\d+?.*$""".r
    regex.findFirstIn(str) != None
  }

  val addInt = (x:Int , y:Int) => x+y
  def doString(x:Int , y:Int) = x+y
//  def do2(x,y) => (Int , Int) = x+y
}
