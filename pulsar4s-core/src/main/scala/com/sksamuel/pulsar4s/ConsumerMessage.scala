package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.impl.MessageImpl
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata
import org.apache.pulsar.shade.io.netty.buffer.Unpooled

import scala.collection.JavaConverters._
import scala.util.Try

case class PublishTime(value: Long)
case class EventTime(value: Long)
case class SequenceId(value: Long)
case class ProducerName(name: String)

trait ConsumerMessage[T] {

  def key: Option[String]

  def value: T

  def valueTry: Try[T] = Try(value)

  def data: Array[Byte]

  /**
    * Return the properties attached to the message.
    * Properties are application defined key/value pairs
    * that will be attached to the message.
    */
  def props: Map[String, String]

  /**
    * Get the unique [[MessageId]] associated with this message.
    * The message id can be used to univocally refer to a message
    * without having the keep the entire payload in memory.
    */
  def messageId: MessageId

  /**
    * Get the sequence id associated with this message.
    */
  def sequenceId: SequenceId

  def producerName: ProducerName

  /**
    * Get the publish time of this message.
    * The publish time is the timestamp that a client
    * published the message.
    */
  def publishTime: PublishTime

  /**
    * Returns the application specified event time
    * for this message. If no event time was specified
    * then this will return an event time of 0.
    */
  def eventTime: EventTime

  def topic: Topic
}

object ConsumerMessage {

  def fromJava[T](message: JMessage[T]): ConsumerMessage[T] = {
    require(message != null)
    ConsumerMessageWithValueTry(
      Option(message.getKey),
      Try(message.getValue),
      message.getData,
      message.getProperties.asScala.toMap,
      MessageId.fromJava(message.getMessageId),
      SequenceId(message.getSequenceId),
      ProducerName(message.getProducerName),
      PublishTime(message.getPublishTime),
      EventTime(message.getEventTime),
      Topic(message.getTopicName)
    )
  }

  def toJava[T](message: ConsumerMessage[T], schema: Schema[T]): JMessage[T] = {
    require(message != null)
    new MessageImpl(message.topic.name, MessageId.toJava(message.messageId).toString, message.props.asJava, Unpooled.wrappedBuffer(message.data), schema, MessageMetadata.newBuilder())
  }
}

case class ConsumerMessageWithValueTry[T](key: Option[String],
                                          override val valueTry: Try[T],
                                          data: Array[Byte],
                                          props: Map[String, String],
                                          messageId: MessageId,
                                          sequenceId: SequenceId,
                                          producerName: ProducerName,
                                          publishTime: PublishTime,
                                          eventTime: EventTime,
                                          topic: Topic) extends ConsumerMessage[T] {
  def value: T = valueTry.get
}


@deprecated("Use ConsumerMessageWithValueTry", "2.4.6")
case class DefaultConsumerMessage[T](key: Option[String],
                                     value: T,
                                     data: Array[Byte],
                                     props: Map[String, String],
                                     messageId: MessageId,
                                     sequenceId: SequenceId,
                                     producerName: ProducerName,
                                     publishTime: PublishTime,
                                     eventTime: EventTime,
                                     topic: Topic) extends ConsumerMessage[T]
