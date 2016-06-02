package org.apache.spark.example

/**
  * Created by admin on 2016/5/30.
  */
object SimpleJob {
  def main(args: Array[String]) {
    val beam = new SimpleEventBeamFactory("10.8.8.230:2181").makeBeam
    val sender = Tranquilizer.create(beam)
    sender.start()
    val future: Future[Unit] = sender.send(SimpleEvent.generateEvents.head)
    future.respond(print(_))
    sender.stop()
  }
}


