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

object PrintSchema{

        def main(args:Array[String]){
                
                 val dirName = args(0)
                 val parameterFile = args(1)
                 var outputDir = args(2)
		 val sparkUrl = args(3)                 


                 val conf = new SparkConf().setAppName("Introspection: GetRowCount")
                 val sc = new SparkContext(sparkUrl, "Introspection: GetRowCount", conf)

                 for(line <- Source.fromFile(parameterFile).getLines()){
                        var arr = line.split(" ")
                        var concept = arr(0)
                        //var inputFileName = concept+"_table.json"
                        var inputFileName = dirName+File.separator+concept+".json"
			
			var sqlContext= new SQLContext(sc)
                  
 	                 val records = sqlContext.read.json(inputFileName)
			records.printSchema

                }
        }


}

