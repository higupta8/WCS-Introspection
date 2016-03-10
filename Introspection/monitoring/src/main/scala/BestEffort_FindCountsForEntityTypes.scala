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

object BestEffort_FindCountsForEntityTypes{
        def getOutputFileName(concept:String) : String = {
                var query = "BestEffortEntityCount"
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

                 

                 val conf = new SparkConf().setAppName("Introspection: BestEffortEntityCount")
                 val sc = new SparkContext(sparkUrl, "Introspection: BestEffortEntityCount", conf)

		 
                 for(line <- Source.fromFile(parameterFile).getLines()){
			var arr = line.split(" ")
                        var concept = arr(0)

			var outputFile = outputDir+"//"+getOutputFileName(concept)
                        FileUtils.deleteQuietly(new File(outputFile))

			scala.tools.nsc.io.File(outputFile).appendAll("Property"+"\t"+"Count"+"\n")


                        var inputFileName = concept+".json"
			getAndPrintCount(sc,dirName+File.separator+inputFileName, outputFile, sparkUrl)
		//	getAndPrintDistinctCount(sc,"GenericEntity",dirName+File.separator+inputFileName,outputFile, sparkUrl)                        

		//	PrintCSVOutput_Methods.appendRDDOfPairs_SL(outputFile, count, "")
                }
        }

        def getAndPrintCount (sc: SparkContext, fileName: String,  outputFile: String, sparkUrl : String) : Unit = {
                //conf.set("spark.driver.memory","2000m")
                //conf.set("spark.executor.memory","4000m"

                var sqlContext= new SQLContext(sc)

                val records = sqlContext.read.json(fileName)

		records.printSchema
		val entityTypesAndCount = records.flatMap(rec=>List(
								   ("Total GenericEntity Instances", getLength(rec, "GenericEntity")),
								   ("Total GenericRelationship Instances", getLength(rec, "GenericRelationship")),
							   ("Total LocationEntity Instances", getLength(rec, "LocationEntity")),
								   ("Total OrganizationEntity Instances",getLength(rec,"OrganizationEntity")),
							   ("Total PersonEntity Instances",getLength(rec,"PersonEntity"))))

		//entityTypesAndCount.collect().foreach(println)
		val enCounts = entityTypesAndCount.reduceByKey((x,y)=>x+y)
		
		PrintCSVOutput_Methods.appendRDDOfPairs_SL(outputFile, enCounts, "")
	
        }

	def getLength (rec: org.apache.spark.sql.Row, enType: String) : Long = {
		val enArr = rec.getAs[scala.collection.Seq[org.apache.spark.sql.Row]](enType)
		if(enArr==null)
			return 0
		else
			return enArr.length
	}

	def getAndPrintDistinctCount(sc: SparkContext, enType: String, fileName: String,  outputFile: String, sparkUrl : String) : Unit = {
		 var sqlContext= new SQLContext(sc)
                 val records = sqlContext.read.json(fileName)
		 
		 //val enArr = records.flatMap(rec=>rec.getAs[scala.collection.Seq[org.apache.spark.sql.Row]](enType))
		 //val enArrNotNull = enArr.filter(rec=>rec!=null)
		// val names = enArrNotNull.map(rec=>rec.getAs[String]("name"))
		 //val distinct_names = names.distinct()

		 scala.tools.nsc.io.File(outputFile).appendAll("Distinct "+enType+" instances"+"\t"+records.count()+"\n")
	}
	
}

