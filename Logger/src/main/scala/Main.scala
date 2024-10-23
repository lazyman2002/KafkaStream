import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
object Main{
	// Định nghĩa biến đồng bộ
	val lock = new Object()
	def main (args : Array[String]) : Unit ={
		val spark = SparkSession.builder()
				.appName("Logger")
				.master("yarn")
				.getOrCreate()
		var df = spark
				.readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", "172.18.0.14:9092,172.18.0.13:9092,172.18.0.12:9092")
				.option("subscribe", "PageView")
				.option("startingOffsets", "latest")
				.load()
		// Chọn cột 'value' từ Kafka
		val processedDF = df.selectExpr("CAST(value AS STRING) as message")
		processedDF.writeStream
				.foreachBatch((batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) => {
					val sb : StringBuffer = new StringBuffer()
					batchDF.collect().foreach { row =>
						val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"))
						val logLevel = "INFO"
						val message = row.getAs[String]("message")
						val logContent = s"$timestamp $logLevel - $message\n"
						sb.append(logContent)
					}
					appendToFile(sb.toString, batchId)
				})
				.option("checkpointLocation", "hdfs://172.18.0.2:9000/user/WBAC/checkpoints/pageview_data")
				.start()
				.awaitTermination()
	}
	def appendToFile(content: String, batchId: Long): Unit = {
		val currentDate = LocalDate.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
		val hadoopConfig : Configuration = new Configuration();
		hadoopConfig.set("fs.defaultFS", "hdfs://172.18.0.2:9000")
//		lock.synchronized{
			val fileSystem = FileSystem.get(hadoopConfig)
			val filePath = s"hdfs://172.18.0.2:9000/user/WBAC/pageviewlog/pageview-$currentDate.log"
			val hdfsPath = new Path(filePath)
			var fileOutputStream : FSDataOutputStream = null
			try if(fileSystem.exists(hdfsPath)) {
				fileOutputStream = fileSystem.append(hdfsPath)
				fileOutputStream.writeBytes(content)
			}
			else {
				fileOutputStream = fileSystem.create(hdfsPath)
				fileOutputStream.writeBytes(content)
			}
			finally {
				if(fileSystem != null) fileSystem.close
				if(fileOutputStream != null) fileOutputStream.close
			}
			fileOutputStream.close()
			fileSystem.close()
//		}
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