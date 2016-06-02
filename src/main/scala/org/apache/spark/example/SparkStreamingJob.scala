package org.apache.spark.example

import com.metamx.tranquility.spark.BeamRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.joda.time.DateTime
/**
  * Created by admin on 2016/5/30.
  */
object SparkStreamingJob {
  def send(rdd: RDD[SimpleEvent], simpleEventBeamFactory: SimpleEventBeamFactory): Unit = {
    val beamRDD : BeamRDD[SimpleEvent] = rdd
    beamRDD.propagate(simpleEventBeamFactory)
    System.out.println("output: " + rdd.count +  " \n"  +  rdd.map(_.toString()).collect().mkString("\n"))

  }

  def main(args: Array[String]) {
    val sparkContext = new SparkContext(
      new SparkConf().setMaster("local[*]").setAppName("SparkDruidTest")
    )
    val ssc = new StreamingContext(sparkContext, Seconds(60))
    val dstream :DStream[SimpleEvent]= ssc.receiverStream(new CustomReceiver)
    val simpleEventBeamFactory = new SimpleEventBeamFactory("10.8.8.230:2181")
    dstream.foreachRDD(rdd  => send(rdd, simpleEventBeamFactory))
    ssc.start()
    ssc.awaitTermination()
  }
  class CustomReceiver()
    extends Receiver[SimpleEvent](StorageLevel.MEMORY_AND_DISK_2) with Logging {
    var flag = true
    override def onStart(): Unit = {
      new Thread("real receiver") {
        override def run() {
          while(flag) {
            store(SimpleEvent.generateEvents().toIterator)
            Thread.sleep(10000)
          }
        }
      }.start()

    }

    override def onStop(): Unit = {
      flag = false
    }
}
