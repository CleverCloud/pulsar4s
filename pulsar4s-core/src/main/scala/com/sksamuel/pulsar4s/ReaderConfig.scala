package com.sksamuel.pulsar4s

import java.util.concurrent.TimeUnit

sealed trait StartMessage
final case class Message(messageId: MessageId) extends StartMessage
final case class RollBack(rollbackDuration: Long, timeunit: TimeUnit) extends StartMessage

case class ReaderConfig(topic: Topic,
                        @deprecated("use startMessage instead", "2.5.3")
                        seek: MessageId = MessageId.earliest,
                        startMessage: StartMessage,
                        startMessageIdInclusive: Boolean = true,
                        receiverQueueSize: Option[Int] = None,
                        reader: Option[String] = None,
                        readCompacted: Option[Boolean] = None,
                        additionalProperties: Map[String, AnyRef] = Map.empty)
