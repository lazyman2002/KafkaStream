import com.google.gson.{Gson, GsonBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Main{
	val gson : Gson = new GsonBuilder().setPrettyPrinting().create()
	def main (args : Array[String]) : Unit ={
		createReplaceTable()
		val spark = SparkSession.builder()
				.appName("KafkaToHbase")
				.master("yarn")
				.getOrCreate()
		var df = spark
				.readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", "172.18.0.14:9092,172.18.0.13:9092,172.18.0.12:9092")
				.option("subscribe", "PageView")
				.option("startingOffsets", "latest")
				.load()
		val schema = newSchemaSpark()
		val processedDF = df.selectExpr("CAST(value AS STRING) as message")
		val pageViewDF = processedDF.select(from_json(col("message"), schema).as("pageView"))
				.select("pageView.*")
		pageViewDF.writeStream
				.foreach(new HBaseForeachWriter)
				.start()
				.awaitTermination()
	}
	private def createReplaceTable(): Unit = {
		val hbaseConfig: Configuration = HBaseConfiguration.create
		hbaseConfig.set("hbase.zookeeper.quorum", "172.18.0.10")
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		hbaseConfig.set("hbase.master", "172.18.0.11:16000")
		
		var connection = ConnectionFactory.createConnection(hbaseConfig)
		var inpTableName = "pageviewlog"
		val admin = connection.getAdmin
		val tableName = TableName.valueOf("pageviewlog")
		
		val tableExists = admin.tableExists(tableName)
		if (tableExists) {
			admin.disableTable(tableName)
			admin.deleteTable(tableName)
		}
		
		if (!admin.tableExists(tableName)) {
			val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
			
			val producerFamily = ColumnFamilyDescriptorBuilder.newBuilder("producer".getBytes).build()
			val consumerFamily = ColumnFamilyDescriptorBuilder.newBuilder("consumer".getBytes).build()
			val hardwareFamily = ColumnFamilyDescriptorBuilder.newBuilder("hardware".getBytes).build()
			
			tableDescriptorBuilder.setColumnFamily(producerFamily)
			tableDescriptorBuilder.setColumnFamily(consumerFamily)
			tableDescriptorBuilder.setColumnFamily(hardwareFamily)
			
			admin.createTable(tableDescriptorBuilder.build())
			println("Bảng đã được tạo thành công!")
		} else {
			println("Bảng đã tồn tại.")
		}
		admin.close()
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