package com.sksamuel.pulsar4s

import java.time.Instant

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.impl.MessageImpl
import org.apache.pulsar.shade.io.netty.buffer.Unpooled

import scala.concurrent.duration.Duration

trait ProducerMessage[T] {

  def key: Option[String]

  def value: T

  /**
    * Return the properties attached to the message.
    * Properties are application defined key/value pairs
    * that will be attached to the message.
    */
  def props: Map[String, String]

  def sequenceId: Option[SequenceId]

  /**
    * Returns the application specified event time
    * for this message. If no event time has been set, then
    * returns None
    */
  def eventTime: Option[EventTime]

  def replicationClusters: List[String]

  def disableReplication: Boolean

  def deliverAt: Option[Long]
}

object ProducerMessage {

  import scala.collection.JavaConverters._

  def apply[T](t: T): ProducerMessage[T] = DefaultProducerMessage[T](None, t)

  def apply[T](key: String, t: T): ProducerMessage[T] = DefaultProducerMessage[T](Some(key), t)

  def apply[T](t: T, deliverAt: Instant): ProducerMessage[T] =
    DefaultProducerMessage[T](None, t, deliverAt = Some(deliverAt.getEpochSecond))

  def apply[T](t: T, deliverAfter: Duration): ProducerMessage[T] =
    DefaultProducerMessage[T](None, t, deliverAt = Some(System.currentTimeMillis + deliverAfter.toMillis))

  def apply[T](key: String, t: T, deliverAt: Instant): ProducerMessage[T] =
    DefaultProducerMessage[T](Some(key), t, deliverAt = Some(deliverAt.getEpochSecond))

  def apply[T](key: String, t: T, deliverAfter: Duration): ProducerMessage[T] =
    DefaultProducerMessage[T](Some(key), t, deliverAt = Some(System.currentTimeMillis + deliverAfter.toMillis))

  def fromJava[T](msg: JMessage[T]): ProducerMessage[T] = {
    DefaultProducerMessage[T](
      Option(msg.getKey),
      msg.getValue,
      msg.getProperties.asScala.toMap,
      Option(msg.getSequenceId).map(SequenceId.apply),
      Option(msg.getEventTime).map(EventTime.apply)
    )
  }

  def toJava[T](msg: ProducerMessage[T], schema: Schema[T]): JMessage[T] = {
    val javaMsg = new MessageImpl(null, null, msg.props.asJava, Unpooled.wrappedBuffer(schema.encode(msg.value)), schema)
    msg.deliverAt foreach javaMsg.getMessageBuilder.setDeliverAtTime
    javaMsg
  }
}

case class DefaultProducerMessage[T](key: Option[String],
                                     value: T,
                                     props: Map[String, String] = Map.empty,
                                     sequenceId: Option[SequenceId] = None,
                                     eventTime: Option[EventTime] = None,
                                     disableReplication: Boolean = false,
                                     replicationClusters: List[String] = Nil,
                                     deliverAt: Option[Long] = None) extends ProducerMessage[T]
