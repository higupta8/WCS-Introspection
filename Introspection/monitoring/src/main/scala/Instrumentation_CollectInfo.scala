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

object Instrumentation_CollectInfo{
        def getOutputFileName(concept:String) : String = {
                var query = "Instrumentation"
                var formatter : SimpleDateFormat = new SimpleDateFormat("dd-MM-yy")
                var datestr = formatter.format(new Date())

                val today = Calendar.getInstance().getTime() 
                val minuteFormat = new SimpleDateFormat("mm")
                val hourFormat = new SimpleDateFormat("HH")
               

                val amPmFormat = new SimpleDateFormat("a")

                val currentHour = hourFormat.format(today)      
                val currentMinute = minuteFormat.format(today) 
                
                val amOrPm = amPmFormat.format(today)         
                return "IntrospectionOutput#"+query+"#"+concept+"#"+datestr+"#"+currentHour+"-"+currentMinute
        }

        def main(args:Array[String]){
                
                 val dirName = args(0)
                 val parameterFile = args(1)
                 var outputDir = args(2)
		 val sparkUrl = args(3)                 

                 

                 val conf = new SparkConf().setAppName("Introspection: CollectInstrumentationInfo")
                 val sc = new SparkContext(sparkUrl, "Introspection: ColeectInstrumentationInfo", conf)

		 
                 for(line <- Source.fromFile(parameterFile).getLines()){
			var arr = line.split(" ")
                        var concept = arr(0)

			var outputFile = outputDir+"//"+getOutputFileName(concept)
                        FileUtils.deleteQuietly(new File(outputFile))

			scala.tools.nsc.io.File(outputFile).appendAll("Property"+"\t"+"Count"+"\n")


                        var inputFileName = concept+".json"
			getAndPrintInfo(sc,dirName+File.separator+inputFileName, outputFile, sparkUrl)
                }
        }

        def getAndPrintInfo(sc: SparkContext, fileName: String,  outputFile: String, sparkUrl : String) : Unit = {
                var sqlContext= new SQLContext(sc)

                val records = sqlContext.read.json(fileName)

		var recordsCount = records.count()
		scala.tools.nsc.io.File(outputFile).appendAll("Count of Records"+"\t"+recordsCount+"\n")		

		val instrumentation_info = records.map(rec=>rec.getAs[org.apache.spark.sql.Row]("instrumentationInfo"))
	
		val runningTimeMS = instrumentation_info.map(rec=>rec.getAs[Long]("runningTimeMS"))
		val total_running_time = runningTimeMS.sum()
		val avg_running_time = runningTimeMS.mean()
		scala.tools.nsc.io.File(outputFile).appendAll("Total Running Time in ms"+"\t"+total_running_time+"\n")
		scala.tools.nsc.io.File(outputFile).appendAll("Average Running Time in ms"+"\t"+avg_running_time+"\n")


		val numAnnotations =  instrumentation_info.map(rec=>rec.getAs[Long]("numAnnotationsTotal"))
		val total_annotations = numAnnotations.sum()
		val avg_annotations = numAnnotations.mean()
		scala.tools.nsc.io.File(outputFile).appendAll("Total Numbe of Annotations"+"\t"+total_annotations+"\n")
                scala.tools.nsc.io.File(outputFile).appendAll("Average Number of Annotations in each Document"+"\t"+avg_annotations+"\n")


		val documentSize_info = instrumentation_info.map(rec=>rec.getAs[Long]("documentSizeChars"))
		val total_size = documentSize_info.sum()
		val avg_size = documentSize_info.mean()
		scala.tools.nsc.io.File(outputFile).appendAll("Size of Document Corpus in Characters"+"\t"+total_size+"\n")
                scala.tools.nsc.io.File(outputFile).appendAll("Average Document Size in Characters"+"\t"+avg_size+"\n")


		val numAnnotationsPerType_info = instrumentation_info.flatMap(rec=>rec.getAs[scala.collection.Seq[org.apache.spark.sql.Row]]("numAnnotationsPerType"))
		val numAnnotationsPerType = numAnnotationsPerType_info.map(rec=>("Total "+rec.getAs[String]("annotationType")+" instances", rec.getAs[Long]("numAnnotations")))
		val annCounts = numAnnotationsPerType.reduceByKey((x,y)=>x+y)
		
		PrintCSVOutput_Methods.appendRDDOfPairs_SL(outputFile, annCounts, "")
	
        }

	def getLength (rec: org.apache.spark.sql.Row, enType: String) : Long = {
		val enArr = rec.getAs[scala.collection.Seq[org.apache.spark.sql.Row]](enType)
		if(enArr==null)
			return 0
		else
			return enArr.length
	}	
}
