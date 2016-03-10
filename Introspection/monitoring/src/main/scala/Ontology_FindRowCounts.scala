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

object Ontology_FindRowCounts{
        def getOutputFileName() : String = {
                var query = "RowCount"
                var formatter : SimpleDateFormat = new SimpleDateFormat("dd-MM-yy")
                var datestr = formatter.format(new Date())

                val today = Calendar.getInstance().getTime() 
                val minuteFormat = new SimpleDateFormat("mm")
                val hourFormat = new SimpleDateFormat("HH")
               

                val amPmFormat = new SimpleDateFormat("a")

                val currentHour = hourFormat.format(today)      
                val currentMinute = minuteFormat.format(today) 
                
                val amOrPm = amPmFormat.format(today)         
                return "IntrospectionOutput#"+query+"#ALL"+"#"+datestr+"#"+currentHour+"-"+currentMinute
        }

        def main(args:Array[String]){
                
                 val dirName = args(0)
                 val parameterFile = args(1)
                 var outputDir = args(2)
		 val sparkUrl = args(3)                 

                 var outputFile = outputDir+"//"+getOutputFileName()
                 FileUtils.deleteQuietly(new File(outputFile))

		 scala.tools.nsc.io.File(outputFile).appendAll("Concept Table"+"\t"+"Row Count"+"\n")

                 val conf = new SparkConf().setAppName("Introspection: GetRowCount")
                 val sc = new SparkContext(sparkUrl, "Introspection: GetRowCount", conf)

                 for(line <- Source.fromFile(parameterFile).getLines()){
                        var arr = line.split(" ")
                        var concept = arr(0)
                        var inputFileName = concept+"_table.json"
                        //var inputFileName = concept+".json"
			var count = getCount(sc,dirName+File.separator+inputFileName, sparkUrl)
                        scala.tools.nsc.io.File(outputFile).appendAll(concept+"\t"+count+"\n")
                }
        }

        def getCount (sc: SparkContext, fileName: String,  sparkUrl : String) : Long = {
               
                //conf.set("spark.driver.memory","2000m")
                //conf.set("spark.executor.memory","4000m"

                var sqlContext= new SQLContext(sc)

                val records = sqlContext.read.json(fileName)

		
		return records.count
        }

}

