package com.sksamuel.pulsar4s

import org.apache.pulsar.client.impl.TopicMessageIdImpl

import scala.language.implicitConversions

/**
 * A wrapper for the Java Pulsar client MessageId.
 *
 * Unfortunately we have to retain the underlying Java object, since some of the Java APIs assume `TopicMessageIdImpl`
 * and perform a type cast from `MessageId`, and this type is not possible to create directly via the public Java API.
 */
sealed trait MessageId {
  def underlying: JMessageId
  def bytes: Array[Byte] = underlying.toByteArray
  def topic: Option[Topic]
  def topicPartition: Option[TopicPartition]
}

private case class Pulsar4sMessageIdImpl(underlying: JMessageId) extends MessageId {
  def topic: Option[Topic] = underlying match {
    case topicMessageId: TopicMessageIdImpl => Some(Topic(topicMessageId.getTopicName))
    case _ => None
  }
  def topicPartition: Option[TopicPartition] = underlying match {
    case topicMessageId: TopicMessageIdImpl => Some(TopicPartition(topicMessageId.getTopicPartitionName))
    case _ => None
  }
  override def toString: String = underlying match {
    case tmi: TopicMessageIdImpl => s"${tmi.getTopicPartitionName} ${tmi.getInnerMessageId}"
    case mi => mi.toString
  }
}

object MessageId {

  val earliest: MessageId = fromJava(org.apache.pulsar.client.api.MessageId.earliest)
  val latest: MessageId = fromJava(org.apache.pulsar.client.api.MessageId.latest)

  implicit def fromJava(messageId: JMessageId): MessageId = Pulsar4sMessageIdImpl(messageId)
  implicit def toJava(messageId: MessageId): JMessageId = messageId.underlying
}
