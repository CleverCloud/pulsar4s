package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api._

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex


case class ConsumerConfig(subscriptionName: Subscription,
                          topics: Seq[Topic] = Nil,
                          topicPattern: Option[Regex] = None,
                          consumerEventListener: Option[ConsumerEventListener] = None,
                          cryptoFailureAction: Option[ConsumerCryptoFailureAction] = None,
                          consumerName: Option[String] = None,
                          cryptoKeyReader: Option[CryptoKeyReader] = None,
                          autoUpdatePartitions: Option[Boolean] = None,
                          maxTotalReceiverQueueSizeAcrossPartitions: Option[Int] = None,
                          negativeAckRedeliveryDelay: Option[FiniteDuration] = None,
                          patternAutoDiscoveryPeriod: Option[Int] = None,
                          priorityLevel: Option[Int] = None,
                          receiverQueueSize: Option[Int] = None,
                          subscriptionInitialPosition: Option[SubscriptionInitialPosition] = None,
                          subscriptionTopicsMode: Option[RegexSubscriptionMode] = None,
                          subscriptionType: Option[SubscriptionType] = None,
                          readCompacted: Option[Boolean] = None,
                          ackTimeout: Option[FiniteDuration] = None,
                          ackTimeoutTickTime: Option[FiniteDuration] = None,
                          acknowledgmentGroupTime: Option[FiniteDuration] = None,
                          additionalProperties: Map[String, AnyRef] = Map.empty,
                          deadLetterPolicy: Option[DeadLetterPolicy] = None)

