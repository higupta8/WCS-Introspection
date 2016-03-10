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


object Ontology_FindStatsForNumericAttributes{
        def getOutputFileName() : String = {
                var query = "BasicStats"
                var formatter : SimpleDateFormat = new SimpleDateFormat("dd-MM-yy")
                var datestr = formatter.format(new Date())

                val today = Calendar.getInstance().getTime()
                val minuteFormat = new SimpleDateFormat("mm")
                val hourFormat = new SimpleDateFormat("HH")

                val currentHour = hourFormat.format(today)     
                val currentMinute = minuteFormat.format(today) 
                    
                return "IntrospectionOutput#"+query+"#ALL"+"#"+datestr+"#"+currentHour+"-"+currentMinute
        }


	 def main(args:Array[String]){
                 val dirName = args(0)
                 val parameterFile = args(1)
                 var outputDir = args(2)
		 val sparkUrl = args(3)

                 var outputFile = outputDir+"//"+getOutputFileName()
                 FileUtils.deleteQuietly(new File(outputFile))

		 scala.tools.nsc.io.File(outputFile).appendAll("Concept and Property"+"\t"+"Basic Statistics"+"\n")                

		 val conf = new SparkConf().setAppName("Introspection: GetBasicStats")
                 val sc = new SparkContext(sparkUrl, "Introspection: GetBasicStats", conf)

                 for(line <- Source.fromFile(parameterFile).getLines()){
                        var arr = line.split(" ")
                        var concept = arr(0)
                        var property = arr(1)
                        var propType = arr(2)
                        var inputFileName = concept+"_table.json"
                        var stats = getBasicStats(sc,dirName+File.separator+inputFileName,property,propType,sparkUrl)
                        scala.tools.nsc.io.File(outputFile).appendAll("("+concept+","+property+")"+"\t"+stats+"\n")
                }
        }


	def getBasicStats(sc: SparkContext, fileName: String,  property: String, propType: String, sparkUrl : String) : String= {
                var sqlContext= new SQLContext(sc)
                val records = sqlContext.jsonFile(fileName)
		
		if(propType=="Long"){
                        val props = records.map(rec=>rec.getAs[Long](property))
                        var min = props.min()
                        var max = props.max()
                        var avg = props.mean()
			var varianc = props.variance()
			return "min="+min+ " max="+max+" avg="+avg+" Top-10 = "+props.top(10).mkString(" ")
                }

                if(propType=="Float"){
                        val props = records.map(rec=>rec.getAs[Float](property))
                        var min = props.min()
                        var max = props.max()
			var avg = props.mean()
			var varianc = props.variance()
                        return "min="+min+ " max="+max+" avg="+avg+" Top-10 = "+props.top(10).mkString(" ")
                }

                if(propType=="Double"){
                        val props = records.map(rec=>rec.getAs[Double](property))
                        var min = props.min()
                        var max = props.max()
			var avg = props.mean()
			var varianc = props.variance()
                        return "min="+min+ " max="+max+" avg="+avg+" Top-10 = "+props.top(10).mkString(" ")
                }
                else{
                        return "NaN"
                }
        }

}

