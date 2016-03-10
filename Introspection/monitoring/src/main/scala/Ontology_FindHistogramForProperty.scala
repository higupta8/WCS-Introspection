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

object Ontology_FindHistogramForProperty{

	 def getOutputFileName(concept: String, property: String) : String = {
                var query = "Histogram"
                var formatter : SimpleDateFormat = new SimpleDateFormat("dd-MM-yy")
                var datestr = formatter.format(new Date())

                val today = Calendar.getInstance().getTime()
                val minuteFormat = new SimpleDateFormat("mm")
                val hourFormat = new SimpleDateFormat("HH")
               
                val currentHour = hourFormat.format(today)      
                val currentMinute = minuteFormat.format(today)  
            
                return "IntrospectionOutput#"+query+"#"+concept+"_"+property+"#"+datestr+"#"+currentHour+"-"+currentMinute
        }

	 def main(args:Array[String]){
                 val dirName = args(0)
                 val parameterFile = args(1)
                 var outputDir = args(2)
		 val sparkUrl = args(3)

               

                 val conf = new SparkConf().setAppName("Introspection: GetHistogram")
                 val sc = new SparkContext(sparkUrl, "Introspection: GetHistogram", conf)

                 for(line <- Source.fromFile(parameterFile).getLines()){
                        var arr = line.split(" ")
                        var concept = arr(0)
                        var property = arr(1)
                        var inputFileName = concept+"_table.json"
                        
                        var outputFile = outputDir+"//"+getOutputFileName(concept, property)
                        FileUtils.deleteQuietly(new File(outputFile))
                        var histogram = getHistogram (sc, dirName+File.separator+inputFileName, property, sparkUrl)
                       	
			scala.tools.nsc.io.File(outputFile).appendAll("Attribute Value"+"\t"+"Count"+"\n")
			PrintCSVOutput_Methods.appendRDDOfPairs_SL(outputFile, histogram,  "Histogram for property "+concept+"_"+property)	
                }
        }

	def getHistogram (sc: SparkContext, fileName: String, property: String, sparkUrl : String) : 
			org.apache.spark.rdd.RDD[(String, Long)] = {	
                var sqlContext= new SQLContext(sc)

                val records = sqlContext.read.json(fileName)
		val properties = records.map(rec=>(rec.getAs[String](property), 1.toLong))
		val counts = properties.reduceByKey((x,y)=>x+y)
		val nonNullCounts = counts.filter(_._1 != null)
		val sortedCounts = nonNullCounts.sortBy(_._2, false)
	               
                return sortedCounts
        }

}



	

