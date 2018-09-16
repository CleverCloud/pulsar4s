package com.sksamuel.pulsar4s

import org.apache.pulsar.client.impl.{MessageIdImpl, TopicMessageIdImpl}

import scala.language.implicitConversions

trait MessageId {
  def bytes: Array[Byte]
}

object MessageId {

  val earliest: MessageId = fromJava(org.apache.pulsar.client.api.MessageId.earliest)
  val latest: MessageId = fromJava(org.apache.pulsar.client.api.MessageId.latest)

  implicit def fromJava(messageId: JMessageId): MessageId = messageId match {
    case batch: MessageIdImpl => StandardMessageId(batch.getLedgerId, batch.getEntryId, batch.getPartitionIndex, batch.toByteArray)
    case topic: TopicMessageIdImpl => TopicMessageId(Topic(topic.getTopicName), fromJava(topic.getInnerMessageId))
    case other => BasicMessageId(other.toByteArray)
  }

  implicit def toJava(messageId: MessageId): JMessageId = {
    org.apache.pulsar.client.api.MessageId.fromByteArray(messageId.bytes)
  }
}

case class BasicMessageId(bytes: Array[Byte]) extends MessageId

case class StandardMessageId(ledgerId: Long, entryId: Long, partitionIndex: Int, bytes: Array[Byte]) extends MessageId

case class TopicMessageId(topic: Topic, messageId: MessageId) extends MessageId {
  override def bytes: Array[Byte] = messageId.bytes
}
