/**
 * Created by Michael on 1/25/16.
 */

import java.io.File
import java.util.Calendar
import java.util.logging._

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object TermVector {


	private val exhaustive = 0

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
			val sparkConf = new SparkConf().setMaster("local[6]")
			sparkConf.setAppName("TermVector_LineageDD")
				.set("spark.executor.memory", "2g")

			//set up lineage
			var lineage = true
			var logFile = "test_log"
			lineage = true

			val ctx = new SparkContext(sparkConf)

			//set up lineage context
			val lc = new LineageContext(ctx)
			lc.setCaptureLineage(lineage)

			//start recording time for lineage
			val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			val LineageStartTime = System.nanoTime()
			logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

			//spark program starts
			val lines = lc.textFile("textFile", 1)
			val wordDoc1 = lines
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
						if (newCount > 10) {
							wordFreqMap = wordFreqMap updated(w, 10000)
						} else
							wordFreqMap = wordFreqMap updated(w, newCount)
					} else {
						if(!w.contains(","))
						wordFreqMap = wordFreqMap + (w -> 1)
					}
				}
				// wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
				wordFreqMap = sortByValue(wordFreqMap)
				(docName, wordFreqMap)
			})
				.filter(pair => {
				if (pair._2.isEmpty) false
				else true
			})

			//	println(wordDoc1.count())
				val wordDoc = wordDoc1.groupByKey()
				//This map mark the ones that could crash the program
				.map(pair => {
				var mark = false
				var value = new String("")
				var totalNum = 0
				for (l <- pair._2) {
					for ((k, v) <- l) {
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
			println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
			//stop the lineage capture
			lc.setCaptureLineage(false)
			Thread.sleep(1000)

			//print out the output for debugging purpose
//			for (o <- out) {
//				println(o._1._1 + " : " + o._1._2 + " - " + o._2)
//			}


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
			}
			}

			linRdd = linRdd.goBackAll()
			//At this stage, technically lineage has already find all the faulty data set, we record the time
			val lineageEndTime = System.nanoTime()
			val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
			logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)


			linRdd = linRdd.goNext()

			val showMeRdd = linRdd.show().toRDD
			println(">>>>>>>>>>>>>  Go Next Done  <<<<<<<<<<<<<<<")
			var mappedRDD = showMeRdd.map(s => {
//				var wordFreqMap: Map[String, Int] = Map()
//				val str = s.toString
//				val index = str.lastIndexOf(",")
//				val lineageID = str.substring(index + 1, str.length - 1)
//				val content = str.substring(2, index - 1)
//				val index2 = content.indexOf(",")
//				val key = content.substring(0, index2)
//				val mapString = content.substring(index2 + 5, content.length - 1)
//				val lst = mapString.trim().split(",")
//				for (s <- lst) {
//					var value = s.toString
//					if (s.substring(0, 1).equals(" ")) value = s.substring(1)
//					val indexa = value.lastIndexOf(">")
//
//					val keya = value.substring(0, indexa - 2)
//					val valuea = value.substring(indexa + 2).toInt
//					wordFreqMap = wordFreqMap + (keya -> valuea)
//				}
//				((key, wordFreqMap), lineageID.toLong)
				(s.asInstanceOf[Tuple2[Any,Any]]._1.asInstanceOf[(String, Map[String, Int])] , 0L)
			})

			val array = mappedRDD.collect()
			//mappedRDD.cache()
			mappedRDD = ctx.parallelize(array)


		//	println("MappedRDD has " + mappedRDD.count() + " records")


			//Remove output before delta-debugging

			val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			val DeltaDebuggingStartTime = System.nanoTime()
			logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

			val delta_debug = new DD_NonEx[(String, Map[String, Int]), Long]
			val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split, lm, fh)

			println(">>>>>>>>>>>>>  DD Done  <<<<<<<<<<<<<<<")
			val ss = returnedRDD.collect.foreach(println)
//			// linRdd.collect.foreach(println)
//			linRdd = wordDoc.getLineage()
//			linRdd.collect
//			linRdd = linRdd.goBack().goBack().filter(l => {
//				if (l.asInstanceOf[(Int, Int)]._2 == ss(0)._2.toInt) {
//					println("*** => " + l)
//					true
//				} else false
//			})
//
//			linRdd = linRdd.goBackAll()
//			linRdd.collect()
//			linRdd.show()

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

