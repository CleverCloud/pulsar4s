package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api._

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

case class ProducerConfig(topic: Topic,
                          encryptionKey: Option[String] = None,
                          batchingMaxBytes: Option[Int] = None,
                          batchingMaxMessages: Option[Int] = None,
                          batchingMaxPublishDelay: Option[FiniteDuration] = None,
                          blockIfQueueFull: Option[Boolean] = None,
                          cryptoFailureAction: Option[ProducerCryptoFailureAction] = None,
                          cryptoKeyReader: Option[CryptoKeyReader] = None,
                          enableBatching: Option[Boolean] = None,
                          hashingScheme: Option[HashingScheme] = None,
                          initialSequenceId: Option[Long] = None,
                          maxPendingMessages: Option[Int] = None,
                          maxPendingMessagesAcrossPartitions: Option[Int] = None,
                          messageRouter: Option[MessageRouter] = None,
                          messageRoutingMode: Option[MessageRoutingMode] = None,
                          roundRobinRouterBatchingPartitionSwitchFrequency: Option[Int] = None,
                          producerName: Option[String] = None,
                          sendTimeout: Option[FiniteDuration] = None,
                          compressionType: Option[CompressionType] = None,
                          additionalProperties: Map[String, AnyRef] = Map.empty)

