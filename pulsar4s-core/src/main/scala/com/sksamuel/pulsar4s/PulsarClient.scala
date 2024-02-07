package com.sksamuel.pulsar4s

import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.conversions.collections._
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.transaction.Transaction
import org.apache.pulsar.client.api.{ConsumerBuilder, ProducerBuilder, ReaderBuilder, Schema}

import java.util.concurrent.TimeUnit
import java.util.{UUID, Set => JSet}
import scala.concurrent.duration._
import scala.util.Success

case class Topic(name: String)

case class TopicPartition(name: String)

case class Subscription(name: String)

object Subscription {
  /**
   * Generates a [[Subscription]] with a random UUID as the name.
   */
  def generate: Subscription = Subscription(UUID.randomUUID.toString)
}

sealed trait TransactionContext {
  /**
   * The underlying transaction associated with this context.
   */
  def transaction: Transaction

  /**
   * Explicitly commit the transaction. Note that this action must occur _after_ all other actions on the transaction.
   */
  def commit[F[_] : AsyncHandler]: F[Unit]

  /**
   * Explicitly abort the transaction. Note that this action must occur _after_ all other actions on the transaction.
   */
  def abort[F[_] : AsyncHandler]: F[Unit]


  /**
   * Get an instance of `TransactionalConsumerOps` that provides transactional operations on the consumer.
   */
  final def apply[T](consumer: Consumer[T]): TransactionalConsumerOps[T] = consumer.tx(this)

  /**
   * Get an instance of `TransactionalProducerOps` that provides transactional operations on the producer.
   */
  final def apply[T](producer: Producer[T]): TransactionalProducerOps[T] = producer.tx(this)
}

object TransactionContext {
  def apply(txn: Transaction): TransactionContext = new TransactionContext {
    lazy val transaction: Transaction = txn

    def commit[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].commitTransaction(txn)

    def abort[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].abortTransaction(txn)
  }
}

sealed trait TransactionBuilder {
  /**
   * Start a transaction.
   */
  def start[F[_] : AsyncHandler]: F[TransactionContext]

  /**
   * Return a new builder with the given timeout.
   */
  def withTimeout(timeout: FiniteDuration): TransactionBuilder

  /**
   * Given a `TransactionContext => F[A]`, produce an `F[A]` that runs in a new transaction.
   *
   * If `F` fails, abort the transaction. Otherwise commit the transaction.
   */
  def runWith[A, F[_] : AsyncHandler](action: TransactionContext => F[A]): F[A]

  /**
   * Given a `TransactionContext => F[Either[E, A]]`, produce an `F[Either[E, A]]` that runs in a new transaction.
   *
   * If `F` fails or the result is a `Left`, abort the transaction. Otherwise commit the transaction.
   */
  def runWithEither[E, A, F[_] : AsyncHandler](action: TransactionContext => F[Either[E, A]]): F[Either[E, A]]
}

private class TransactionBuilderImpl(
                                      client: org.apache.pulsar.client.api.PulsarClient,
                                      timeout: FiniteDuration = 60.seconds
                                    ) extends TransactionBuilder {
  private def javaBuilder: api.transaction.TransactionBuilder =
    client.newTransaction().withTransactionTimeout(timeout.length, timeout.unit)

  override def start[F[_] : AsyncHandler]: F[TransactionContext] =
    implicitly[AsyncHandler[F]].startTransaction(javaBuilder)

  override def withTimeout(timeout: FiniteDuration): TransactionBuilder =
    new TransactionBuilderImpl(client, timeout)

  override def runWith[T, F[_] : AsyncHandler](action: TransactionContext => F[T]): F[T] = {
    val async = implicitly[AsyncHandler[F]]
    async.transform(runWithEither { ctx =>
      async.transform[T, Either[T, T]](action(ctx))(r => Success(Right(r)))
    })(r => Success(r.merge))
  }

  override def runWithEither[E, A, F[_] : AsyncHandler](action: TransactionContext => F[Either[E, A]]): F[Either[E, A]] =
    implicitly[AsyncHandler[F]].withTransaction(javaBuilder, action)
}

trait PulsarClient {
  def close(): Unit

  def producer[T: Schema](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]] = Nil): Producer[T]

  def consumer[T: Schema](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]] = Nil): Consumer[T]

  def reader[T: Schema](config: ReaderConfig): Reader[T]
}

trait PulsarAsyncClient extends PulsarClient {
  def closeAsync[F[_] : AsyncHandler]: F[Unit]

  def producerAsync[T: Schema, F[_] : AsyncHandler](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]] = Nil): F[Producer[T]]

  def consumerAsync[T: Schema, F[_] : AsyncHandler](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]] = Nil): F[Consumer[T]]

  def readerAsync[T: Schema, F[_] : AsyncHandler](config: ReaderConfig): F[Reader[T]]

  def transaction: TransactionBuilder
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

@deprecated
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

  def apply(config: PulsarClientConfig): PulsarAsyncClient = {
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
    config.enableTransaction.foreach(builder.enableTransaction)
    if (config.additionalProperties.nonEmpty)
      builder.loadConf(config.additionalProperties.asJava)

    new DefaultPulsarClient(builder.build())
  }

  def apply(serviceUrl: String): PulsarAsyncClient = apply(PulsarClientConfig(serviceUrl))
}

class DefaultPulsarClient(client: org.apache.pulsar.client.api.PulsarClient) extends PulsarAsyncClient with Logging {

  override def close(): Unit = client.close()

  private def producerBuilder[T](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]])(implicit schema: Schema[T]): ProducerBuilder[T] = {
    logger.debug(s"Creating producer with config $config")
    val builder = client.newProducer(schema)
    builder.topic(config.topic.name)
    config.encryptionKey.foreach(builder.addEncryptionKey)
    config.batchingMaxBytes.foreach(builder.batchingMaxBytes)
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
    config.roundRobinRouterBatchingPartitionSwitchFrequency.foreach(builder.roundRobinRouterBatchingPartitionSwitchFrequency)
    config.messageRouter.foreach(builder.messageRouter)
    config.messageRoutingMode.foreach(builder.messageRoutingMode)
    config.producerName.foreach(builder.producerName)
    config.sendTimeout.map(_.toMillis.toInt).foreach(builder.sendTimeout(_, TimeUnit.MILLISECONDS))
    config.batcherBuilder.foreach(builder.batcherBuilder)
    if (interceptors.nonEmpty)
      builder.intercept(interceptors.map(new ProducerInterceptorAdapter(_, schema)): _*)
    if (config.additionalProperties.nonEmpty)
      builder.loadConf(config.additionalProperties.asJava)
    builder
  }

  private def consumerBuilder[T](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]] = Nil)(implicit schema: Schema[T]): ConsumerBuilder[T] = {
    logger.info(s"Creating consumer with config $config")
    val builder = client.newConsumer(schema).replicateSubscriptionState(config.replicateSubscriptionState)
    config.consumerEventListener.foreach(builder.consumerEventListener)
    config.consumerName.foreach(builder.consumerName)
    config.cryptoFailureAction.foreach(builder.cryptoFailureAction)
    config.cryptoKeyReader.foreach(builder.cryptoKeyReader)
    config.maxTotalReceiverQueueSizeAcrossPartitions.foreach(builder.maxTotalReceiverQueueSizeAcrossPartitions)
    config.negativeAckRedeliveryDelay.foreach { d => builder.negativeAckRedeliveryDelay(d._1, d._2) }
    config.autoUpdatePartitions.foreach(builder.autoUpdatePartitions)
    config.patternAutoDiscoveryPeriod.foreach { d => builder.patternAutoDiscoveryPeriod(d.length.toInt, d.unit) }
    config.priorityLevel.foreach(builder.priorityLevel)
    config.receiverQueueSize.foreach(builder.receiverQueueSize)
    config.readCompacted.foreach(builder.readCompacted)
    config.subscriptionInitialPosition.foreach(builder.subscriptionInitialPosition)
    config.subscriptionTopicsMode.foreach(builder.subscriptionTopicsMode)
    config.subscriptionType.foreach(builder.subscriptionType)
    config.subscriptionMode.foreach(builder.subscriptionMode)
    config.topicPattern.map(_.pattern).foreach(builder.topicsPattern)
    config.ackTimeout.foreach { t => builder.ackTimeout(t._1, t._2) }
    config.ackTimeoutTickTime.foreach { tt => builder.ackTimeoutTickTime(tt._1, tt._2) }
    config.enableBatchIndexAcknowledgement.foreach { enabled => builder.enableBatchIndexAcknowledgment(enabled) }
    config.deadLetterPolicy.foreach(builder.deadLetterPolicy)
    config.batchReceivePolicy.foreach(builder.batchReceivePolicy)
    config.acknowledgmentGroupTime.foreach { gt => builder.acknowledgmentGroupTime(gt._1, gt._2) }
    if (config.topics.nonEmpty)
      builder.topics(config.topics.map(_.name).asJava)
    builder.subscriptionName(config.subscriptionName.name)
    if (interceptors.nonEmpty)
      builder.intercept(interceptors.map(new ConsumerInterceptorAdapter(_, schema)): _*)
    if (config.additionalProperties.nonEmpty)
      builder.loadConf(config.additionalProperties.asJava)

    logger.info(s"Creating consumer with builder ${builder.toString}")

    builder
  }

  private def readerBuilder[T](config: ReaderConfig)(implicit schema: Schema[T]): ReaderBuilder[T] = {
    logger.info(s"Creating reader with config $config")
    val builder = client.newReader(schema)
    builder.topic(config.topic.name)
    config.reader.foreach(builder.readerName)
    config.startMessage match {
      case Message(messageId) => builder.startMessageId(MessageId.toJava(messageId))
      case RollBack(rollbackDuration, timeunit) => builder.startMessageFromRollbackDuration(rollbackDuration, timeunit)
    }
    if (config.startMessageIdInclusive) {
      builder.startMessageIdInclusive()
    }

    config.receiverQueueSize.foreach(builder.receiverQueueSize)
    config.readCompacted.foreach(builder.readCompacted)
    if (config.additionalProperties.nonEmpty)
      builder.loadConf(config.additionalProperties.asJava)

    builder
  }

  override def producer[T: Schema](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]] = Nil): Producer[T] =
    new DefaultProducer(producerBuilder(config, interceptors).create())

  override def consumer[T: Schema](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]] = Nil): Consumer[T] =
    new DefaultConsumer(consumerBuilder(config, interceptors).subscribe())

  override def reader[T: Schema](config: ReaderConfig): Reader[T] =
    new DefaultReader(readerBuilder(config).create())

  override def closeAsync[F[_] : AsyncHandler]: F[Unit] =
    implicitly[AsyncHandler[F]].close(client)

  override def producerAsync[T: Schema, F[_] : AsyncHandler](config: ProducerConfig, interceptors: List[ProducerInterceptor[T]] = Nil): F[Producer[T]] =
    implicitly[AsyncHandler[F]].createProducer(producerBuilder(config, interceptors))

  override def consumerAsync[T: Schema, F[_] : AsyncHandler](config: ConsumerConfig, interceptors: List[ConsumerInterceptor[T]]): F[Consumer[T]] =
    implicitly[AsyncHandler[F]].createConsumer(consumerBuilder(config, interceptors))

  override def readerAsync[T: Schema, F[_] : AsyncHandler](config: ReaderConfig): F[Reader[T]] = {
    implicitly[AsyncHandler[F]].createReader(readerBuilder(config))
  }

  override def transaction: TransactionBuilder = new TransactionBuilderImpl(client)
}
