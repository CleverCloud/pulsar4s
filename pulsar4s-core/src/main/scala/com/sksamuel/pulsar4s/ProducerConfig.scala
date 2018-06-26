package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.CompressionType

case class ProducerConfig(encryptionKey: Option[String],
                          blockIfQueueFull: Option[Boolean],
                          compressionType: Option[CompressionType])

case class ConsumerConfig(consumerName: Option[String],
                          readCompacted: Option[Boolean])

case class ReaderConfig(receiverQueueSize: Option[Int],
                        reader: Option[String],
                        readCompacted: Option[Boolean])