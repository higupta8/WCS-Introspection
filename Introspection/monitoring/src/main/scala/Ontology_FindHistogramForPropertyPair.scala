import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import java.io.File
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.commons.io.FileUtils
import scala.io.Source


object Ontology_FindHistogramForPropertyPair{
         def getOutputFileName(concept: String, property1: String, property2: String) : String = {
                var query = "HistogramForPropertyPair"
                var formatter : SimpleDateFormat = new SimpleDateFormat("dd-MM-yy")
                var datestr = formatter.format(new Date())

                val today = Calendar.getInstance().getTime()
                val minuteFormat = new SimpleDateFormat("mm")
                val hourFormat = new SimpleDateFormat("HH")
               

                val amPmFormat = new SimpleDateFormat("a")

                val currentHour = hourFormat.format(today)     
                val currentMinute = minuteFormat.format(today) 
                val amOrPm = amPmFormat.format(today)           
                return "IntrospectionOutput#"+query+"#"+concept+"_"+property1+"_"+property2+"#"+datestr+"#"+currentHour+"-"+currentMinute
        }


	
         def main(args:Array[String]){
                 val dirName = args(0)
                 val parameterFile = args(1)
                 var outputDir = args(2)
		 val sparkUrl = args(3)
                 val conf = new SparkConf().setAppName("Introspection: GetHistogramForPropertyPair")
                 val sc = new SparkContext(sparkUrl, "Introspection: GetHistogramForPropertyPair", conf)

                 for(line <- Source.fromFile(parameterFile).getLines()){
                        var arr = line.split(" ")
                        var concept = arr(0)
                        var property1 = arr(1)
                        var property2 = arr(2)
                        var inputFileName = concept+"_table.json"
                        
                        var outputFile = outputDir+"//"+getOutputFileName(concept, property1, property2)
                        FileUtils.deleteQuietly(new File(outputFile))
                        var histogram = getHistogram (sc, dirName+File.separator+inputFileName, property1, property2, sparkUrl)
                        
			scala.tools.nsc.io.File(outputFile).appendAll("Attribute Value Pair"+"\t"+"Count"+"\n")
			PrintCSVOutput_Methods.appendRDDOfPairs_SSL(outputFile, histogram, "Histogram for property pair "+property1+","+property2)
                }
         }

	   def getHistogram(sc: SparkContext, fileName: String, property1: String, property2:String, sparkUrl : String) : org.apache.spark.rdd.RDD[((String, String), Long)] = {
                var sqlContext= new SQLContext(sc)

                val records = sqlContext.read.json(fileName)
                val properties = records.map(rec=>((rec.getAs[String](property1),rec.getAs[String](property2)),1.toLong))
                val nonNullProps = properties.filter(rec=>(rec._1)._1!=null && (rec._1)._2!=null)

                val counts = nonNullProps.reduceByKey((x,y)=>x+y)
		val sortedCounts = counts.sortBy(_._2, false)

                return sortedCounts
        }
}


