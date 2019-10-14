package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api._

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

case class ProducerConfig(topic: Topic,
                          encryptionKey: Option[String] = None,
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
                          producerName: Option[String] = None,
                          sendTimeout: Option[FiniteDuration] = None,
                          compressionType: Option[CompressionType] = None)

case class ConsumerConfig(subscriptionName: Subscription,
                          topics: Seq[Topic] = Nil,
                          topicPattern: Option[Regex] = None,
                          consumerEventListener: Option[ConsumerEventListener] = None,
                          cryptoFailureAction: Option[ConsumerCryptoFailureAction] = None,
                          consumerName: Option[String] = None,
                          cryptoKeyReader: Option[CryptoKeyReader] = None,
                          maxTotalReceiverQueueSizeAcrossPartitions: Option[Int] = None,
                          negativeAckRedeliveryDelay: Option[FiniteDuration] = None,
                          patternAutoDiscoveryPeriod: Option[Int] = None,
                          priorityLevel: Option[Int] = None,
                          receiverQueueSize: Option[Int] = None,
                          subscriptionInitialPosition: Option[SubscriptionInitialPosition] = None,
                          subscriptionType: Option[SubscriptionType] = None,
                          readCompacted: Option[Boolean] = None,
                          ackTimeout: Option[FiniteDuration] = None,
                          ackTimeoutTickTime: Option[FiniteDuration] = None,
                          acknowledgmentGroupTime: Option[FiniteDuration] = None)

case class ReaderConfig(topic: Topic,
                        seek: MessageId,
                        receiverQueueSize: Option[Int] = None,
                        reader: Option[String] = None,
                        readCompacted: Option[Boolean] = None)
