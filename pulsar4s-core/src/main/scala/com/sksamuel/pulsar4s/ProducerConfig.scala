package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api._

case class ProducerConfig(topic: Topic,
                          encryptionKey: Option[String] = None,
                          batchingMaxMessages: Option[Int] = None,
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
                          compressionType: Option[CompressionType] = None)

case class ConsumerConfig(topics: Seq[Topic],
                          subscriptionName: Subscription,
                          consumerEventListener: Option[ConsumerEventListener] = None,
                          cryptoFailureAction: Option[ConsumerCryptoFailureAction] = None,
                          consumerName: Option[String] = None,
                          cryptoKeyReader: Option[CryptoKeyReader] = None,
                          maxTotalReceiverQueueSizeAcrossPartitions: Option[Int] = None,
                          patternAutoDiscoveryPeriod: Option[Int] = None,
                          priorityLevel: Option[Int] = None,
                          receiverQueueSize: Option[Int] = None,
                          subscriptionInitialPosition: Option[SubscriptionInitialPosition] = None,
                          subscriptionType: Option[SubscriptionType] = None,
                          readCompacted: Option[Boolean] = None)

case class ReaderConfig(receiverQueueSize: Option[Int] = None,
                        reader: Option[String] = None,
                        readCompacted: Option[Boolean] = None)