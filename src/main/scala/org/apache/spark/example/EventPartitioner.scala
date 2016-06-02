package org.apache.spark.example

import com.google.common.hash.Hashing
import com.metamx.common.scala.Logging
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.partition.Partitioner
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import java.util.concurrent.atomic.AtomicBoolean
import java.{util => ju}
import org.scala_tools.time.Imports._
import scala.collection.JavaConverters._

/**
  * Created by peibin on 2016/5/27.
  */
class EventPartitioner[A](
                           timestamper: Timestamper[A],
                           timestampSpec: TimestampSpec,
                           rollup: DruidRollup
                         ) extends Partitioner[A] with Logging {
  @transient private val didWarn = new AtomicBoolean

  override def partition(thing: A, numPartitions: Int): Int = {
    val dimensions = thing match {
      case scalaMap: collection.Map[_, _] =>
        for ((k: String, v) <- scalaMap if rollup.isStringDimension(timestampSpec, k)) yield {
          (k, v)
        }

      case javaMap: ju.Map[_, _] =>
        for ((k: String, v) <- javaMap.asScala if rollup.isStringDimension(timestampSpec, k)) yield {
          (k, v)
        }
      case event: SimpleEvent =>
        for ((k: String, v) <- event.toMap if rollup.isStringDimension(timestampSpec, k)) yield {
          (k, v)
        }

      case obj  =>
        // Oops, we don't really know how to do things other than Maps...
        if (didWarn.compareAndSet(false, true)) {
          log.warn(
            "Cannot partition object of class[%s] by time and dimensions. Consider implementing a Partitioner.",
            obj.getClass
          )
        }
        Nil
    }

    val partitionHashCode = if (dimensions.nonEmpty) {
      val truncatedTimestamp = rollup.indexGranularity.truncate(timestamper.timestamp(thing).millis)
      Partitioner.timeAndDimsHashCode(truncatedTimestamp, dimensions)
    } else {
      thing.hashCode()
    }

    Hashing.consistentHash(partitionHashCode, numPartitions)
  }
}