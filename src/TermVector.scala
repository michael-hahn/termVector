/**
 * Created by Michael on 1/25/16.
 */
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.RDD
import scala.Tuple2
import java.util.Calendar
import java.util.StringTokenizer

import scala.collection.mutable.MutableList

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._

object TermVector {

//    def main(args:Array[String]): Unit = {
//      val sparkConf = new SparkConf().setMaster("local[8]")
//      sparkConf.setAppName("InvertedIndex_LineageDD-" )
//        .set("spark.executor.memory", "2g")
//
//      val ctx = new SparkContext(sparkConf)
//
//      val lines = ctx.textFile("../textFile", 1)
//      val constr = new sparkOperations
//      val output = constr.sparkWorks(lines).collect
//      val itr = output.iterator
//      while (itr.hasNext) {
//        val tupVal = itr.next()
//        println(tupVal._1 + ": -")
//        val itr2 = tupVal._2.toIterator
//        while (itr2.hasNext){
//          val tupVal2 = itr2.next()
//          println(tupVal2._1 + " : " + tupVal2._2)
//        }
//
//      }
//      ctx.stop()
//    }


  private val exhaustive = 0

//  def mapFunc(str: String): (String, (String, Int)) = {
//    val token = new StringTokenizer(str)
//    val docName = token.nextToken()
//    val wordWithCount = token.nextToken()
//
//    val colonIndex = wordWithCount.indexOf(":")
//    val word = wordWithCount.substring(0, colonIndex)
//    val count = wordWithCount.substring(colonIndex + 1).toInt
//    return (docName, (word, count))
//  }

  def sortByValue(map: Map[String, Int]): Map[String, Int] = {
    val list: List[(String, Int)] = map.toList.sortWith((x, y) => {
      if (x._2 > y._2) true
      else false
    })
    list.toMap
  }

  def main(args: Array[String]): Unit = {
    try {
      //set up logger
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[8]")
      sparkConf.setAppName("TermVector_LineageDD")
        .set("spark.executor.memory", "2g")

      //set up lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "test_log"
        lineage = true
      } else {
        lineage = args(0).toBoolean
        logFile += args(1)
        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      }

      val ctx = new SparkContext(sparkConf)

      //set up lineage context
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)

      //Prepare for Hadoop MapReduce for correctness test
//      val clw = new commandLineOperations()
//      clw.commandLineWorks()
//      //Run Hadoop to have a groundTruth
//      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/TermVector.jar", "org.apache.hadoop.examples.TermVectorPerHost", "-m", "3", "-r", "1","/Users/Michael/IdeaProjects/textFile", "output").!!

      //start recording time for lineage
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //spark program starts
      val lines = lc.textFile("../textFile.data", 1)
      val wordDoc = lines
        .map(s => {
          var wordFreqMap: Map[String, Int] = Map()
          val wordDocList: MutableList[(String, Int)] = MutableList()
          val colonIndex = s.indexOf(":")
          val docName = s.substring(0, colonIndex)
          val content = s.substring(colonIndex + 1)
          val wordList = content.trim.split(" ")
          for (w <- wordList) {
            if (wordFreqMap.contains(w)) {
              val newCount = wordFreqMap(w) + 1
              wordFreqMap = wordFreqMap updated(w, newCount)
            } else {
              wordFreqMap = wordFreqMap + (w -> 1)
            }
          }
          wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
          wordFreqMap = sortByValue(wordFreqMap)
          (docName, wordFreqMap)
        })
        .filter(pair => {
          if (pair._2.isEmpty) false
          else true
        })
        .groupByKey()
        //This map mark the ones that could crash the program
        .map(pair => {
          var mark = false
          var value = new String("")
          var totalNum = 0
          for (l <- pair._2) {
            for ((k,v) <- l) {
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

      val out = wordDoc.collectWithId()

      //stop the lineage capture
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the output for debugging purpose
//      for (o <- out) {
//        println(o._1._1 + " : " + o._1._2 + " - " + o._2)
//      }


//      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/TermVector/lineageResult"))

      var list = List[Long]()
      for (o <- out) {
        if (o._1._2.substring(o._1._2.length - 1).equals("*")) {
          println(o._1._1 + " : " + o._1._2 + " - " + o._2)
          list = o._2 :: list
        }
      }

      //print the list for debugging
//      println("****************************")
//      for (l <- list) {
//        println(l)
//      }
//      println("****************************")


      var linRdd = wordDoc.getLineage()
      linRdd.collect

      linRdd = linRdd.filter { l => {
        //println("***" + l + "***") //debug
        list.contains(l)
      } }

      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

//      linRdd.show.collect().foreach(s => {
//        pw.append(s.toString)
//        pw.append('\n')
//      })
//
//      pw.close()

      linRdd = linRdd.goNext()

      val showMeRdd = linRdd.show().toRDD

      val mappedRDD = showMeRdd.map(s => {
        var wordFreqMap: Map[String, Int] = Map()
        val str = s.toString
        val index = str.lastIndexOf(",")
        val lineageID = str.substring(index + 1, str.length - 1)
        val content = str.substring(2, index - 1)
        val index2 = content.indexOf(",")
        val key = content.substring(0, index2)
        val mapString = content.substring(index2 + 5, content.length - 1)
        val lst = mapString.trim().split(",")
        for (s <- lst) {
          var value = s.toString
          if (s.substring(0, 1).equals(" ")) value = s.substring(1)
          val indexa = value.lastIndexOf(">")
          val keya = value.substring(0, indexa - 2)
          val valuea = value.substring(indexa + 2).toInt
          wordFreqMap = wordFreqMap + (keya -> valuea)
        }
        ((key, wordFreqMap), lineageID.toLong)
      })

      println("MappedRDD has " + mappedRDD.count() + " records")


//      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/TermVector/lineageResult", 1)
//      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/textFile", 1)
//
//      val num = lineageResult.count()
//      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/TermVector/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
//      lineageResult.cache()

//      if (exhaustive == 1) {
//        val delta_debug: DD[String] = new DD[String]
//        delta_debug.ddgen(lineageResult, new Test,
//          new Split, lm, fh)
//      } else {
        val delta_debug = new DD_NonEx[(String, Map[String, Int]), Long]
        val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split, lm, fh)
//      }

      val ss = returnedRDD.collect
      // linRdd.collect.foreach(println)
      linRdd = wordDoc.getLineage()
      linRdd.collect
      linRdd = linRdd.goBack().goBack().filter(l => {
        if(l.asInstanceOf[(Int, Int)]._2 == ss(0)._2.toInt){
          println("*** => " + l)
          true
        }else false
      })

      linRdd = linRdd.goBackAll()
      linRdd.collect()
      linRdd.show()

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }

      println("Job's DONE! Works!")
      ctx.stop()

    }
  }
}

