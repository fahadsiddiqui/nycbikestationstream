import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{Logging, SparkConf}
import play.api.libs.json.Json

import scala.io.Source

// dynamic station information mapping for json https://gbfs.citibikenyc.com/gbfs/en/station_information.json
case class DynamicStation(station_id: String, num_bikes_available: Long, num_bikes_disabled: Long, num_docks_available: Long, num_docks_disabled: Long, is_installed: Long, is_renting: Long, is_returning: Long, last_reported: Option[Long], eightd_has_available_keys: Boolean)
object DynamicStation {
  implicit val jsonFormat = Json.format[DynamicStation]
}
case class DynamicStationData(stations: Seq[DynamicStation])
object DynamicStationData {
  implicit val jsonFormat = Json.format[DynamicStationData]
}
case class DynamicStationContent(last_updated: Long, ttl: Long, data: DynamicStationData)
object DynamicStationContent {
  implicit val jsonFormat = Json.format[DynamicStationContent]
}

// static station information mapping for json https://gbfs.citibikenyc.com/gbfs/en/station_information.json
case class StaticStation(station_id: String, name: String, short_name: String, lat: BigDecimal, lon: BigDecimal, region_id: Long, rental_methods: Seq[String], capacity: Long, eightd_has_key_dispenser: Boolean)
object StaticStation {
  implicit val jsonFormat = Json.format[StaticStation]
}
case class StaticStationData(stations: Seq[StaticStation])
object StaticStationData {
  implicit val jsonFormat = Json.format[StaticStationData]
}
case class StaticStationContent(last_updated: Long, ttl: Long, data: StaticStationData)
object StaticStationContent {
  implicit val jsonFormat = Json.format[StaticStationContent]
}

object Driver {
  def main(args: Array[String]): Unit = {
    val STATIC_STATIONS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

    val jsonString = new HttpRequest().getHttpRestContentSimple(STATIC_STATIONS_URL)
    val staticStationContent = Json.fromJson[StaticStationContent](Json.parse(jsonString)).get

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("nycbikestationstream")

    val ssc = new StreamingContext(conf, Seconds(5))
    val stream = ssc.receiverStream(new CustomReceiver)
    stream.print
    stream.saveAsTextFiles(s"${args(0)}/live_data")

    ssc.start
    ssc.awaitTermination
  }
}

class CustomReceiver
  extends Receiver[DynamicStationContent](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  val DYNAMIC_STATIONS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

  def onStart {
    new Thread("API data stream") {
      override def run { receive }
    }.start
  }

  def onStop { }

  private def receive {
    while (!isStopped) {
      val content = new HttpRequest().getHttpRestContentSimple(DYNAMIC_STATIONS_URL)
      val stationsData = Json.fromJson[DynamicStationContent](Json.parse(content)).get
      store(stationsData)
    }
  }
}

class HttpRequest {
  def getHttpRestContentSimple(url: String) =
    Source.fromURL(url).mkString
}