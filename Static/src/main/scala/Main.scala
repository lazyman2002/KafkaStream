
import com.google.gson.{Gson, GsonBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main{
	val gson : Gson = new GsonBuilder().setPrettyPrinting().create()
	
	def main (args : Array[String]) : Unit ={
		val spark = SparkSession.builder()
				.appName("Logger")
				.master("yarn")
				.getOrCreate()
		val schema = newSchemaSpark()
		var df = spark
				.readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", "172.18.0.14:9092,172.18.0.13:9092,172.18.0.12:9092")
				.option("subscribe", "PageView")
				.option("startingOffsets", "latest")
				.load()
    
		val filteredDF = df
                .filter(col("timestamp") >= current_timestamp() - expr("INTERVAL 10 MINUTES"))
                .selectExpr("CAST(value AS STRING) AS message")
		
//		totalAccess(filteredDF)
		
//		mostAccess(schema, filteredDF)
	}
	
	private def totalAccess (filteredDF : DataFrame) : Unit ={
		val countQuery = filteredDF
				.groupBy()
				.count()
				.writeStream
				.outputMode("complete")
				.format("console")
				.trigger(Trigger.ProcessingTime("1 minute"))
				.start()
		
		countQuery.awaitTermination()
	}
	
	private def mostAccess (schema : StructType, filteredDF : DataFrame) : Unit ={
		val domainCounts = filteredDF
				.select(from_json(col("message"), schema).as("data")) // Assuming you have a schema defined
				.select("data.domain") // Extract the domain field
				.groupBy("domain") // Group by domain
				.count() // Count occurrences of each domain
				.orderBy(desc("count")) // Order by count descending
				.limit(1)
		val query = domainCounts.writeStream
				.outputMode("complete") // Use complete mode to see the top result
				.format("console")
				.trigger(Trigger.ProcessingTime("1 minute")) // Adjust trigger as needed
				.start()
		
		query.awaitTermination()
	}
	
	def newSchemaSpark () : StructType ={
		val schema =  StructType(Array(
			StructField("timeCreate", DataTypes.TimestampType, nullable = false),
			StructField("cookieCreate", DataTypes.TimestampType, nullable = false),
			StructField("browserCode", DataTypes.IntegerType, nullable = false),
			StructField("browserVer", DataTypes.StringType, nullable = false),
			StructField("osCode", DataTypes.IntegerType, nullable = false),
			StructField("osVer", DataTypes.StringType, nullable = false),
			StructField("ip", DataTypes.LongType, nullable = false),
			StructField("locId", DataTypes.IntegerType, nullable = false),
			StructField("domain", DataTypes.StringType, nullable = false),
			StructField("siteId", DataTypes.IntegerType, nullable = false),
			StructField("cId", DataTypes.IntegerType, nullable = false),
			StructField("path", DataTypes.StringType, nullable = false),
			StructField("referer", DataTypes.StringType, nullable = false),
			StructField("guid", DataTypes.LongType, nullable = false),
			StructField("flashVersion", DataTypes.StringType, nullable = false),
			StructField("jre", DataTypes.StringType, nullable = false),
			StructField("sr", DataTypes.StringType, nullable = false),
			StructField("sc", DataTypes.StringType, nullable = false),
			StructField("geographic", DataTypes.IntegerType, nullable = false),
			StructField("category", DataTypes.StringType, nullable = false)
		))
		schema
	}
}