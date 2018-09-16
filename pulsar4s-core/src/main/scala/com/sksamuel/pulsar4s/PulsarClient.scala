package com.sksamuel.pulsar4s

import java.util.UUID

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
  def reader[T](topic: Topic, seek: MessageId, config: ReaderConfig)(implicit schema: Schema[T]): Reader[T]
}

object PulsarClient {

  def apply(config: PulsarClientConfig): PulsarClient = {
    val builder = org.apache.pulsar.client.api.PulsarClient.builder().serviceUrl(config.serviceUrl)
    config.ioThreads.foreach(builder.ioThreads)
    config.allowTlsInsecureConnection.foreach(builder.allowTlsInsecureConnection)
    config.connectionsPerBroker.foreach(builder.connectionsPerBroker)
    config.enableTcpNoDelay.foreach(builder.enableTcpNoDelay)
    config.enableTls.foreach(builder.enableTls)
    config.enableTlsHostnameVerification.foreach(builder.enableTlsHostnameVerification)
    config.listenerThreads.foreach(builder.listenerThreads)
    config.maxConcurrentLookupRequests.foreach(builder.maxConcurrentLookupRequests)
    config.maxNumberOfRejectedRequestPerConnection.foreach(builder.maxNumberOfRejectedRequestPerConnection)
    config.tlsTrustCertsFilePath.foreach(builder.tlsTrustCertsFilePath)
    new DefaultPulsarClient(builder.build())
  }

  def apply(url: String): PulsarClient = {
    new DefaultPulsarClient(org.apache.pulsar.client.api.PulsarClient.builder().serviceUrl(url).build())
  }
}

class DefaultPulsarClient(client: org.apache.pulsar.client.api.PulsarClient) extends PulsarClient with Logging {

  override def close(): Unit = client.close()

  override def producer[T](config: ProducerConfig)(implicit schema: Schema[T]): Producer[T] = {
    logger.info(s"Creating producer with config $config")
    val builder = client.newProducer(schema)
    builder.topic(config.topic.name)
    config.encryptionKey.foreach(builder.addEncryptionKey)
    config.blockIfQueueFull.foreach(builder.blockIfQueueFull)
    config.compressionType.foreach(builder.compressionType)
    config.batchingMaxMessages.foreach(builder.batchingMaxMessages)
    config.blockIfQueueFull.foreach(builder.blockIfQueueFull)
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
    builder.topics(config.topics.map(_.name).asJava)
    builder.subscriptionName(config.subscriptionName.name)
    config.readCompacted.foreach(builder.readCompacted)
    new DefaultConsumer(builder.subscribe())
  }

  override def reader[T](topic: Topic, seek: MessageId, config: ReaderConfig)(implicit schema: Schema[T]): Reader[T] = {
    logger.info(s"Creating read on $topic with config $config and seek $seek")
    val builder = client.newReader(schema)
    builder.topic(topic.name)
    config.reader.foreach(builder.readerName)
    config.receiverQueueSize.foreach(builder.receiverQueueSize)
    config.readCompacted.foreach(builder.readCompacted)
    new Reader(builder.create(), topic)
  }
}