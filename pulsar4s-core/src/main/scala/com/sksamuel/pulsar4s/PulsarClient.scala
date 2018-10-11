package com.sksamuel.pulsar4s

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api.Schema

import scala.collection.JavaConverters._

case class Topic(name: String)

case class Subscription(name: String)

object Subscription {

  /**
    * Generates a [[Subscription]] with a random UUID as the name.
    */
  def generate: Subscription = Subscription(UUID.randomUUID.toString)
}

trait PulsarClient {
  def close(): Unit

  def producer[T](config: ProducerConfig)(implicit schema: Schema[T]): Producer[T]
  def consumer[T](config: ConsumerConfig)(implicit schema: Schema[T]): Consumer[T]
  def reader[T](config: ReaderConfig)(implicit schema: Schema[T]): Reader[T]
}

object PulsarClient {

  def apply(config: PulsarClientConfig): PulsarClient = {
    val builder = org.apache.pulsar.client.api.PulsarClient.builder().serviceUrl(config.serviceUrl)
    config.ioThreads.foreach(builder.ioThreads)
    config.allowTlsInsecureConnection.foreach(builder.allowTlsInsecureConnection)
    config.authentication.foreach(builder.authentication)
    config.connectionsPerBroker.foreach(builder.connectionsPerBroker)
    config.enableTcpNoDelay.foreach(builder.enableTcpNoDelay)
    config.enableTls.foreach(builder.enableTls)
    config.enableTlsHostnameVerification.foreach(builder.enableTlsHostnameVerification)
    config.ioThreads.foreach(builder.ioThreads)
    config.listenerThreads.foreach(builder.listenerThreads)
    config.maxConcurrentLookupRequests.foreach(builder.maxConcurrentLookupRequests)
    config.maxLookupRequests.foreach(builder.maxLookupRequests)
    config.maxNumberOfRejectedRequestPerConnection.foreach(builder.maxNumberOfRejectedRequestPerConnection)
    config.operationTimeout.map(_.toSeconds.toInt).foreach(builder.operationTimeout(_, TimeUnit.SECONDS))
    config.keepAliveInterval.map(_.toSeconds.toInt).foreach(builder.keepAliveInterval(_, TimeUnit.SECONDS))
    config.statsInterval.map(_.toMillis).foreach(builder.statsInterval(_, TimeUnit.MILLISECONDS))
    config.tlsTrustCertsFilePath.foreach(builder.tlsTrustCertsFilePath)
    new DefaultPulsarClient(builder.build())
  }

  def apply(serviceUrl: String): PulsarClient = apply(PulsarClientConfig(serviceUrl))
}

class DefaultPulsarClient(client: org.apache.pulsar.client.api.PulsarClient) extends PulsarClient with Logging {

  override def close(): Unit = client.close()

  override def producer[T](config: ProducerConfig)(implicit schema: Schema[T]): Producer[T] = {
    logger.info(s"Creating producer with config $config")
    val builder = client.newProducer(schema)
    builder.topic(config.topic.name)
    config.encryptionKey.foreach(builder.addEncryptionKey)
    config.batchingMaxMessages.foreach(builder.batchingMaxMessages)
    config.batchingMaxPublishDelay.map(_.toMillis).foreach(builder.batchingMaxPublishDelay(_, TimeUnit.MILLISECONDS))
    config.blockIfQueueFull.foreach(builder.blockIfQueueFull)
    config.compressionType.foreach(builder.compressionType)
    config.cryptoFailureAction.foreach(builder.cryptoFailureAction)
    config.cryptoKeyReader.foreach(builder.cryptoKeyReader)
    config.enableBatching.foreach(builder.enableBatching)
    config.hashingScheme.foreach(builder.hashingScheme)
    config.initialSequenceId.foreach(builder.initialSequenceId)
    config.maxPendingMessages.foreach(builder.maxPendingMessages)
    config.maxPendingMessagesAcrossPartitions.foreach(builder.maxPendingMessagesAcrossPartitions)
    config.messageRouter.foreach(builder.messageRouter)
    config.messageRoutingMode.foreach(builder.messageRoutingMode)
    config.producerName.foreach(builder.producerName)
    config.sendTimeout.map(_.toSeconds.toInt).foreach(builder.sendTimeout(_, TimeUnit.MILLISECONDS))
    new DefaultProducer(builder.create())
  }

  override def consumer[T](config: ConsumerConfig)(implicit schema: Schema[T]): Consumer[T] = {
    logger.info(s"Creating consumer with config $config")
    val builder = client.newConsumer(schema)
    config.consumerEventListener.foreach(builder.consumerEventListener)
    config.consumerName.foreach(builder.consumerName)
    config.cryptoFailureAction.foreach(builder.cryptoFailureAction)
    config.cryptoKeyReader.foreach(builder.cryptoKeyReader)
    config.maxTotalReceiverQueueSizeAcrossPartitions.foreach(builder.maxTotalReceiverQueueSizeAcrossPartitions)
    config.patternAutoDiscoveryPeriod.foreach(builder.patternAutoDiscoveryPeriod)
    config.priorityLevel.foreach(builder.priorityLevel)
    config.receiverQueueSize.foreach(builder.receiverQueueSize)
    config.readCompacted.foreach(builder.readCompacted)
    config.subscriptionInitialPosition.foreach(builder.subscriptionInitialPosition)
    config.subscriptionType.foreach(builder.subscriptionType)
    config.topicPattern.map(_.pattern).foreach { pattern =>
      builder.topicsPattern(pattern)
    }
    if (config.topics.nonEmpty)
      builder.topics(config.topics.map(_.name).asJava)
    builder.subscriptionName(config.subscriptionName.name)
    new DefaultConsumer(builder.subscribe())
  }

  override def reader[T](config: ReaderConfig)(implicit schema: Schema[T]): Reader[T] = {
    logger.info(s"Creating reader for config $config")
    val builder = client.newReader(schema)
    builder.topic(config.topic.name)
    config.reader.foreach(builder.readerName)
    builder.startMessageId(MessageId.toJava(config.seek))
    config.receiverQueueSize.foreach(builder.receiverQueueSize)
    config.readCompacted.foreach(builder.readCompacted)
    new DefaultReader(builder.create(), config.topic)
  }
}