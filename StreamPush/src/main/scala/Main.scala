import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.seqAsJavaListConverter

object Main{
	def main (args : Array[String]) : Unit ={
		val folderPath = "/home/WBAC/training4/data/sample_text"
		val folder = new File(folderPath)
		val listOfFiles = folder.listFiles
		
		
		val spark = SparkSession.builder()
				.appName("StreamPush")
				.master("yarn")
				.getOrCreate()
		var df = readData(listOfFiles)(spark)
//		val gson = new Gson()

//		val props : Properties = new Properties()
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.14:9092")
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//		val producer = new KafkaProducer[String, String](props)
//		val ds = df
//				.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		val chunkSize : Int = 10
		val totalRows : Long = df.count
		val numBatches : Int = Math.ceil(totalRows.toDouble / chunkSize).toInt
		var i = 0
		while(i < numBatches) {
			val batchDF = df.limit(chunkSize)
			val kafkaDF = batchDF.selectExpr("to_json(struct(*)) AS value")
			batchDF
					.selectExpr("to_json(struct(*)) AS value")
					.write
					.format("kafka")
					.option("kafka.bootstrap.servers", "172.18.0.14:9092,172.18.0.13:9092,172.18.0.12:9092")
					.option("topic", "PageView")
					.option("checkpointLocation", "/home/WBAC/checkpoint")
					.save()
			Thread.sleep(10000)
			i += 1
		}
	}
	
	private def readData (listOfFiles : Array[File])(implicit spark : SparkSession) : DataFrame ={
		var rows = ListBuffer[Row]()
		var pgs = new util.ArrayList[PageView]
		if(listOfFiles != null) {
			for (file <- listOfFiles) {
				try {
					val bufferedReader : BufferedReader = new BufferedReader(new FileReader(file))
					var line : String = null
					while( {
						line = bufferedReader.readLine();
						line != null
					}) {
						val values : Array[String] = line.split("\t")
						val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
						val date1 = dateFormat.parse(values(0))
						val sqlTimeCreate: Timestamp = new Timestamp(date1.getTime)
						val date2 = dateFormat.parse(values(1))
						val sqlCookieCreate: Timestamp = new Timestamp(date2.getTime)
						val row : Row = Row(sqlTimeCreate, //timeCreate
							sqlCookieCreate, //cookieCreate
							values(2).toInt, //browserCode
							values(3), //browserVer
							values(4).toInt, //osCode
							values(5), //osVer ,
							values(6).toLong, //ip
							values(7).toInt, //locId
							values(8), //domain
							values(9).toInt, //siteId
							values(10).toInt, //cId
							values(11), //path
							values(12), //referer
							values(13).toLong, //guid
							values(14), //flashVersion
							values(15), //jre
							values(16), //sr
							values(17), //sc
							values(18).toInt, //geographic
							values(23) //category
						)
						rows += row
					}
				}
				catch {
					case e : FileNotFoundException => println("Couldn't find that file.")
					case e : IOException => println("Had an IOException trying to read that file")
				}
			}
		}
		val finalRows = rows.toList.asJava
		val schema = newSchemaSpark()
		val df = spark.createDataFrame(finalRows ,schema)
		df
	}
	
	private def newSchemaSpark () : StructType ={
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