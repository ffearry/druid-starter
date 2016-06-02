package org.apache.spark.example

/**
  * Created by peibin on 2016/5/26.
  */

import com.fasterxml.jackson.annotation.JsonValue
import com.metamx.common.Granularity
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.beam.{ClusteredBeamTuning, Beam}
import com.metamx.tranquility.druid.{SpecificDruidDimensions, DruidRollup, DruidLocation, DruidBeams}
import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.LongSumAggregatorFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.DateTime
import org.scala_tools.time.Imports._

case class SimpleEvent(val ts: DateTime, val foo: String, val bar: Int, val lat: Double, val lon: Double) {
  @JsonValue
  def toMap: Map[String, Any] = Map(
    "ts" -> ts,
    "foo" -> foo,
    "bar" -> bar,
    "lat" -> lat,
    "lon" -> lon
  )

  def toNestedMap: Map[String, Any] = Map(
    "ts" -> ts,
    "data" -> Map("foo" -> foo, "bar" -> bar),
    "geo" -> Map("lat" -> lat, "lon" -> lon)
  )

  def toCsv: String = Seq(ts, foo, bar, lat, lon).mkString(",")

  override def toString: String = toMap.toString
}

object SimpleEvent {
  implicit def convertToMap(simpleEvent: SimpleEvent) : Map[String, Any] =  simpleEvent.toMap
  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) =  new DateTime(a.ts)
  }
  val Columns = Seq("ts", "foo", "bar", "lat", "lon")
  def generateEvents(): Seq[SimpleEvent] = {
    //    generateEvents(new DateTime(DateTimeZone.UTC).hourOfDay().roundFloorCopy())
    generateEvents((new DateTime().toDateTime(DateTimeZone.UTC)))
  }
  def generateEvents(now: DateTime): Seq[SimpleEvent] = {
    Seq(
      SimpleEvent(now, "hey", 2, 37.7833, -122.4167),
      SimpleEvent(now, "hey2", 4, 37.7833, -122.4167),
      SimpleEvent(now, "what", 3, 37.7833, 122.4167)
    )
  }
}
class SimpleEventBeamFactory(zkConnect: String) extends BeamFactory[SimpleEvent]
{
  def makeBeam: Beam[SimpleEvent] = SimpleEventBeamFactory.mkBeam(zkConnect)
}

object SimpleEventBeamFactory
{
  private val beams = mutable.HashMap[String, Beam[SimpleEvent]]()
  // Return a singleton object for every zookeeper, so the same connection is shared across all tasks in the same JVM.
  def mkBeam(zkConnect: String) : Beam[SimpleEvent] = {
    beams.synchronized{
      beams.getOrElseUpdate(zkConnect, instance(zkConnect))
    }
  }

  def instance(zkConnect: String): Beam[SimpleEvent] = {
    val curator = CuratorFrameworkFactory.newClient(
      zkConnect,
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path

    val dataSource = "foo1107"
    val dimensions = IndexedSeq("foo")
    val aggregators = Seq(new LongSumAggregatorFactory("bar", "bar"))

    val timestampSpec =  new TimestampSpec("ts", "auto", null)
    val rollup = DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularity.MINUTE)
    val partitioner = new EventPartitioner[SimpleEvent](SimpleEvent.simpleEventTimestamper, timestampSpec, rollup)

    DruidBeams
      .builder[SimpleEvent]()(SimpleEvent.simpleEventTimestamper)
      .curator(curator)
      .timestampSpec(timestampSpec)
      .partitioner(partitioner)
      .discoveryPath(discoveryPath)
      .location(DruidLocation.create(indexService,dataSource))
      .rollup(rollup)
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period("PT10M"),
          partitions = 1,
          replicants = 1
        )
      )
      .buildBeam()

  }
}