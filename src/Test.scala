/**
 * Created by Michael on 1/25/16.
 */

import java.io._
import java.util.logging.{Level, FileHandler, LogManager, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Test extends userTest[(String, Map[String, Int])] with Serializable {
var num =0;
  def usrTest(inputRDD: RDD[(String, Map[String, Int])], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass, which returns false
    var returnValue = false


 //   inputRDD.collect().foreach(println)
    val finalRdd = inputRDD
         .groupByKey()
         //This map mark the ones that could crash the program
         .map(pair => {
      var mark = false
      var value = new String("")
      var totalNum = 0
      val array = pair._2.toList.asInstanceOf[List[(Map[String,Int],Long)]]
      for (l <- array) {
        for ((k, v) <- l._1) {
          value += k + "-" + v + ","
          totalNum += v
        }
      }
      value = value.substring(0, value.length - 1)
      val ll = value.split(",")
      if (totalNum / ll.size > 5) mark = true
      if (mark) value += "*"
      (pair._1, value)
    })
    val start = System.nanoTime

    val out = finalRdd.collect()
    logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000 + "")
    num = num +1
    logger.log(Level.INFO, "TestRuns :" + num + "")
    println(s""">>>>>>>>>>>>>>>>>>>>>>>>>>   Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<""")
    for (o <- out) {
     // println(o)
      if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
    }
    returnValue
  }
}
