package com.sksamuel.pulsar4s

import java.util.UUID
import java.util.{Set => JSet}
import java.util.concurrent.TimeUnit

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.Schema

import scala.collection.JavaConverters._

case class Topic(name: String)

case class TopicPartition(name: String)

case class Subscription(name: String)

object Subscription {

  /**
    * Generates a [[Subscription]] with a random UUID as the name.
    */
  def generate: Subscription = Subscription(UUID.randomUUID.toString)
}

trait PulsarClient {
  def close(): Unit
  def producer[T](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]] = Nil)(implicit schema: Schema[T]): Producer[T]
  def consumer[T](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]] = Nil)(implicit schema: Schema[T]): Consumer[T]
  def reader[T](config: ReaderConfig)(implicit schema: Schema[T]): Reader[T]
}

trait ProducerInterceptor[T] extends AutoCloseable {
  def beforeSend(message: ProducerMessage[T]): ProducerMessage[T]
  def onAck(message: ProducerMessage[T], messageId: MessageId): Unit
  def onError(message: ProducerMessage[T], t: Throwable): Unit
}

trait ConsumerInterceptor[T] extends AutoCloseable {
  def beforeConsume(message: ConsumerMessage[T]): ConsumerMessage[T]
  def onAck(messageId: MessageId): Unit
  def onError(messageId: MessageId, throwable: Throwable): Unit
  def onAckCumulative(messageId: MessageId): Unit
  def onErrorCumulative(messageId: MessageId, throwable: Throwable): Unit
  def onNegativeAcksSend(messageIds: Set[MessageId]): Unit
  def onAckTimeoutSend(messageIds: Set[MessageId]): Unit
}

class ConsumerInterceptorAdapter[T](interceptor: ConsumerInterceptor[T], schema: Schema[T]) extends api.ConsumerInterceptor[T] {

  override def close(): Unit = interceptor.close()

  override def beforeConsume(consumer: api.Consumer[T], message: JMessage[T]): JMessage[T] = {
    val intercepted = interceptor.beforeConsume(ConsumerMessage.fromJava(message))
    ConsumerMessage.toJava(intercepted, schema)
  }

  override def onAcknowledge(consumer: api.Consumer[T], messageId: JMessageId, throwable: Throwable): Unit = {
    if (throwable == null) interceptor.onAck(MessageId.fromJava(messageId)) else interceptor.onError(MessageId.fromJava(messageId), throwable)
  }

  override def onAcknowledgeCumulative(consumer: api.Consumer[T], messageId: JMessageId, throwable: Throwable): Unit = {
    if (throwable == null) interceptor.onAckCumulative(MessageId.fromJava(messageId)) else interceptor.onErrorCumulative(MessageId.fromJava(messageId), throwable)
  }

  override def onNegativeAcksSend(consumer: JConsumer[T], set: JSet[JMessageId]): Unit = {
    interceptor.onNegativeAcksSend(set.asScala.map(MessageId.fromJava).toSet)
  }

  override def onAckTimeoutSend(consumer: JConsumer[T], set: JSet[JMessageId]): Unit = {
    interceptor.onAckTimeoutSend(set.asScala.map(MessageId.fromJava).toSet)
  }
}

class ProducerInterceptorAdapter[T](interceptor: ProducerInterceptor[T], schema: Schema[T]) extends api.ProducerInterceptor[T] {

  override def close(): Unit = interceptor.close()

  override def beforeSend(producer: api.Producer[T], msg: JMessage[T]): JMessage[T] = {
    val intercepted = interceptor.beforeSend(ProducerMessage.fromJava(msg))
    ProducerMessage.toJava(intercepted, schema)
  }

  override def onSendAcknowledgement(producer: api.Producer[T], msg: JMessage[T], messageId: JMessageId, throwable: Throwable): Unit = {
    if (throwable == null)
      interceptor.onAck(ProducerMessage.fromJava(msg), MessageId.fromJava(messageId))
    else
      interceptor.onError(ProducerMessage.fromJava(msg), throwable)
  }
}

object PulsarClient {

  def apply(config: PulsarClientConfig): PulsarClient = {
    val builder = org.apache.pulsar.client.api.PulsarClient.builder().serviceUrl(config.serviceUrl)
    config.ioThreads.foreach(builder.ioThreads)
    config.allowTlsInsecureConnection.foreach(builder.allowTlsInsecureConnection)
    config.authentication.foreach(builder.authentication)
    config.connectionsPerBroker.foreach(builder.connectionsPerBroker)
    config.enableTcpNoDelay.foreach(builder.enableTcpNoDelay)
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

  override def producer[T](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]] = Nil)(implicit schema: Schema[T]): Producer[T] = {
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
    config.sendTimeout.map(_.toMillis.toInt).foreach(builder.sendTimeout(_, TimeUnit.MILLISECONDS))
    if (interceptors.nonEmpty)
      builder.intercept(interceptors.map(new ProducerInterceptorAdapter(_, schema)): _*)
    new DefaultProducer(builder.create())
  }

  override def consumer[T](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]] = Nil)(implicit schema: Schema[T]): Consumer[T] = {
    logger.info(s"Creating consumer with config $config")
    val builder = client.newConsumer(schema)
    config.consumerEventListener.foreach(builder.consumerEventListener)
    config.consumerName.foreach(builder.consumerName)
    config.cryptoFailureAction.foreach(builder.cryptoFailureAction)
    config.cryptoKeyReader.foreach(builder.cryptoKeyReader)
    config.maxTotalReceiverQueueSizeAcrossPartitions.foreach(builder.maxTotalReceiverQueueSizeAcrossPartitions)
    config.negativeAckRedeliveryDelay.foreach { d => builder.negativeAckRedeliveryDelay(d._1, d._2) }
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
    if (interceptors.nonEmpty)
      builder.intercept(interceptors.map(new ConsumerInterceptorAdapter(_, schema)): _*)
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
