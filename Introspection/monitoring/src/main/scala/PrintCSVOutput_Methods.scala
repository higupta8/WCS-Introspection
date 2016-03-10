import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.commons.io.FileUtils

object PrintCSVOutput_Methods{
        def printMapOfCounts (outputFileName: String, counts: scala.collection.Map[String, Long], propertyName: String) : Unit = {
                FileUtils.deleteQuietly(new File(outputFileName))
                val numelements = counts.size

                for ((k,v) <- counts){
                               scala.tools.nsc.io.File(outputFileName).appendAll(k+"\t"+v+"\n")
                }
        }

 	def appendMapOfCounts (outputFileName: String, counts: scala.collection.Map[String, Long], propertyName: String) : Unit = {
                val numelements = counts.size

                for ((k,v) <- counts){
                               scala.tools.nsc.io.File(outputFileName).appendAll(k+"\t"+v+"\n")
                }
        }



        def printRDDOfPairs_SSL (outputFileName: String, counts: org.apache.spark.rdd.RDD[((String, String),Long)], propertyName: String) : Unit = {
		FileUtils.deleteQuietly(new File(outputFileName))
                counts.collect().foreach(kv=>
                        scala.tools.nsc.io.File(outputFileName).appendAll(kv._1+"\t"+kv._2+"\n")
                )

        }

	def appendRDDOfPairs_SSL (outputFileName: String, counts: org.apache.spark.rdd.RDD[((String, String),Long)], propertyName: String) : Unit = {
               
                counts.collect().foreach(kv=>
                        scala.tools.nsc.io.File(outputFileName).appendAll(kv._1+"\t"+kv._2+"\n")
                )

        }


	def printRDDOfPairs_SL (outputFileName: String, counts: org.apache.spark.rdd.RDD[(String,Long)], propertyName: String) : Unit = {
                FileUtils.deleteQuietly(new File(outputFileName))
                counts.collect().foreach(kv=>
                        scala.tools.nsc.io.File(outputFileName).appendAll(kv._1+"\t"+kv._2+"\n")
                )

        }


	def appendRDDOfPairs_SL (outputFileName: String, counts: org.apache.spark.rdd.RDD[(String,Long)], propertyName: String) : Unit = {
                counts.collect().foreach(kv=>
                        scala.tools.nsc.io.File(outputFileName).appendAll(kv._1+"\t"+kv._2+"\n")
                )

        }

}


