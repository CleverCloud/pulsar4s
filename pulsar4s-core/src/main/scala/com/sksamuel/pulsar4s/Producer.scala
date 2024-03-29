package com.sksamuel.pulsar4s

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api.transaction.Transaction
import org.apache.pulsar.client.api.{ProducerStats, TypedMessageBuilder}

import java.io.Closeable
import scala.util.Try

/**
  * Operations on the producer that may be used in a transactional context.
  */
trait TransactionalProducerOps[T] {

  /**
    * Asynchronously sends a message of type T, returning an effect
    * which will be completed with the [[MessageId]] once the message
    * is acknowledged by the Pulsar broker.
    *
    * This method can be used when you have no need to set the
    * other properties of a message, such as the event time, key,
    * headers and so on. The producer will generate an appropriate
    * Pulsar [[ProducerMessage]] with this t set as the value.
    */
  def sendAsync[F[_] : AsyncHandler](t: T): F[MessageId]

  /**
    * Asynchronously sends a [[ProducerMessage]] of type T, returning an effect
    * which will be completed with the [[MessageId]] once the message
    * is acknowledged by the Pulsar broker.
    *
    * This method can be used when you want to specify properties
    * on a message such as the event time, key and so on.
    */
  def sendAsync[F[_] : AsyncHandler](msg: ProducerMessage[T]): F[MessageId]
}

trait Producer[T] extends Closeable with Logging with TransactionalProducerOps[T] {

  /**
    * Returns the [[ProducerName]] which could have been specified
    * by the client or assigned by the system.
    */
  def name: ProducerName

  /**
    * Sends a message of type T.
    * The message sent will have no key.
    *
    * This method can be used when you have no need to set the
    * other properties of a message, such as the event time, key,
    * headers and so on. The producer will generate an appropriate
    * Pulsar [[ProducerMessage]] with this t set as the value.
    *
    * This call will block until it is successfully acknowledged by
    * the Pulsar broker.
    */
  def send(t: T): Try[MessageId]

  /**
    * Sends a [[ProducerMessage]] of type T.
    * This method can be used when you want to specify properties
    * on a message such as the event time, key and so on.
    *
    * This call will block until it is successfully acknowledged by
    * the Pulsar broker.
    */
  def send(msg: ProducerMessage[T]): Try[MessageId]

  /**
    * Get the last sequence id that was published by this producer.
    *
    * This represented either the automatically assigned or custom
    * sequence id that was published and acknowledged by the broker.
    *
    * After recreating a producer with the same producer name,
    * this will return the last message that was published in
    * the previous producer session,
    * or -1 if there no message was ever published.
    */
  def lastSequenceId: SequenceId

  def stats: ProducerStats

  /**
    * Returns the [[Topic]] that a producer is publishing to.
    */
  def topic: Topic

  /**
    * Close the [[Producer]] and releases resources allocated.
    *
    * No more writes will be accepted from this producer.
    * Waits until all pending write request are persisted. In case
    * of errors, pending writes will not be retried.
    */
  def close(): Unit

  /**
    * Close the [[Producer]], releases resources allocated, and
    * returns an effect that is completed when the close operation
    * has completed.
    *
    * No more writes will be accepted from this producer.
    * Waits until all pending write request are persisted. In case
    * of errors, pending writes will not be retried.
    */
  def closeAsync[F[_] : AsyncHandler]: F[Unit]

  def isConnected: Boolean

  def flush(): Unit

  def flushAsync[F[_] : AsyncHandler]: F[Unit]

  /**
    * Get an instance of `TransactionalProducerOps` that provides transactional operations on the producer.
    */
  def tx(implicit ctx: TransactionContext): TransactionalProducerOps[T]
}

class DefaultProducer[T](producer: JProducer[T]) extends Producer[T] {

  override def name: ProducerName = ProducerName(producer.getProducerName)

  override final def send(t: T): Try[MessageId] = send(ProducerMessage(t))
  override final def sendAsync[F[_] : AsyncHandler](t: T): F[MessageId] = sendAsync(ProducerMessage(t))

  override def send(msg: ProducerMessage[T]): Try[MessageId] = Try(buildMessage(msg).send())
  override def sendAsync[F[_] : AsyncHandler](msg: ProducerMessage[T]): F[MessageId] = AsyncHandler[F].send(buildMessage(msg))

  override def lastSequenceId: SequenceId = SequenceId(producer.getLastSequenceId)
  override def stats: ProducerStats = producer.getStats
  override def topic: Topic = Topic(producer.getTopic)

  override def isConnected: Boolean = producer.isConnected

  override def flush(): Unit = producer.flush()
  override def flushAsync[F[_] : AsyncHandler]: F[Unit] = AsyncHandler[F].flush(producer)

  override def close(): Unit = {
    logger.info("Closing producer")
    producer.close()
  }

  override def closeAsync[F[_] : AsyncHandler]: F[Unit] = AsyncHandler[F].close(producer)

  override def tx(implicit ctx: TransactionContext): TransactionalProducerOps[T] =
    new DefaultTransactionalProducer[T](producer, ctx.transaction)

  protected def buildMessage(msg: ProducerMessage[T]): TypedMessageBuilder[T] =
    new ProducerMessageBuilder(producer, None).build(msg)
}

class DefaultTransactionalProducer[T](producer: JProducer[T], transaction: Transaction) extends DefaultProducer[T](producer) {
  override protected def buildMessage(msg: ProducerMessage[T]): TypedMessageBuilder[T] =
    new ProducerMessageBuilder[T](producer, Some(transaction)).build(msg)
}

class ProducerMessageBuilder[T](producer: JProducer[T], transaction: Option[Transaction]) {
  def build(msg: ProducerMessage[T]): TypedMessageBuilder[T] = {
    import conversions.collections._
    
    val builder = transaction.fold(producer.newMessage())(producer.newMessage).value(msg.value)
    msg.deliverAt.foreach { da =>
      builder.deliverAt(da)
    }
    msg.key.foreach(builder.key)
    msg.sequenceId.map(_.value).foreach(builder.sequenceId)
    msg.eventTime.map(_.value).foreach(builder.eventTime)
    if (msg.replicationClusters.nonEmpty)
      builder.replicationClusters(msg.replicationClusters.asJava)
    if (msg.disableReplication)
      builder.disableReplication
    for ((key, value) <- msg.props) {
      builder.property(key, value)
    }
    builder
  }
}
