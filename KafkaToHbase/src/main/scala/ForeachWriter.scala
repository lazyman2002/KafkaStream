import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{ForeachWriter, Row}

import java.sql.Timestamp

class HBaseForeachWriter extends ForeachWriter[Row] {
	private var table: Table = _
	
	override def open(partitionId: Long, version: Long): Boolean = {
		val conf = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum", "172.18.0.10")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		val connection = ConnectionFactory.createConnection(conf)
		table = connection.getTable(TableName.valueOf("pageviewlog"))
		true
	}
	
	override def process(record: Row): Unit = {
		// Extract fields from Row and create a PageView
		val pageView = new PageView(
			record.getAs[Timestamp]("timeCreate"),
			record.getAs[Timestamp]("cookieCreate"),
			record.getAs[Int]("browserCode"),
			record.getAs[String]("browserVer"),
			record.getAs[Int]("osCode"),
			record.getAs[String]("osVer"),
			record.getAs[Long]("ip"),
			record.getAs[Int]("locId"),
			record.getAs[String]("domain"),
			record.getAs[Int]("siteId"),
			record.getAs[Int]("cId"),
			record.getAs[String]("path"),
			record.getAs[String]("referer"),
			record.getAs[Long]("guid"),
			record.getAs[String]("flashVersion"),
			record.getAs[String]("jre"),
			record.getAs[String]("sr"),
			record.getAs[String]("sc"),
			record.getAs[Int]("geographic"),
			record.getAs[String]("category")
		)
		
		// Create Put object with row key using the guid
		val put = new Put(Bytes.toBytes((String.valueOf(pageView.hashCode()))))
		
		// hardware column family
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("browserCode"), Bytes.toBytes(pageView.getBrowserCode))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("browserVer"), Bytes.toBytes(pageView.getBrowserVer))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("osCode"), Bytes.toBytes(pageView.getOsCode))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("osVer"), Bytes.toBytes(pageView.getOsVer))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("flashVersion"), Bytes.toBytes(pageView.getFlashVersion))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("jre"), Bytes.toBytes(pageView.getJre))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("sr"), Bytes.toBytes(pageView.getSr))
		put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("sc"), Bytes.toBytes(pageView.getSc))
		
		// consumer column family
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("cId"), Bytes.toBytes(pageView.getcId))
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("guid"), Bytes.toBytes(pageView.getGuid))
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("ip"), Bytes.toBytes(pageView.getIp))
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("locId"), Bytes.toBytes(pageView.getLocId))
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("geographic"), Bytes.toBytes(pageView.getGeographic))
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("timeCreate"), Bytes.toBytes(pageView.getTimeCreate.toString))
		put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(pageView.getCookieCreate.toString))
		
		// producer column family
		put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("domain"), Bytes.toBytes(pageView.getDomain))
		put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("path"), Bytes.toBytes(pageView.getPath))
		put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("referer"), Bytes.toBytes(pageView.getReferer))
		put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("siteId"), Bytes.toBytes(pageView.getSiteId))
		put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("category"), Bytes.toBytes(pageView.getCategory))
		
		table.put(put)
	}
	
	override def close(errorOrNull: Throwable): Unit = {
		if (table != null) {
			table.close()
		}
	}
}
