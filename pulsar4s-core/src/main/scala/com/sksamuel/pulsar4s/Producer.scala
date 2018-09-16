package com.sksamuel.pulsar4s

import java.io.Closeable

import org.apache.pulsar.client.api.{ProducerStats, Schema, Producer => JProducer}

import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

case class SequenceId(value: Long)

case class ProducerName(name: String)

trait Producer[T] extends Closeable {

  /**
    * Returns the [[ProducerName]] which could have been specified
    * by the client or assigned by the system.
    */
  def name: ProducerName

  /**
    * Sends a message of type T.
    * This call will block until it is successfully acknowledged by
    * the Pulsar broker.
    */
  def send(t: T): MessageId

  /**
    * Asynchronously sends a message of type T return an effect of type F
    * which will be completed when the message is acknowledged by the
    * Pulsar broker.
    */
  def sendAsync[F[_] : AsyncHandler](t: T): F[MessageId]

  def trySend(t: T): Try[MessageId] = Try(send(t))

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
}

class DefaultProducer[T](producer: JProducer[T])(implicit schema: Schema[T]) extends Producer[T] {

  override def name: ProducerName = ProducerName(producer.getProducerName)

  override def send(t: T): MessageId = MessageId.fromJava(producer.send(t))
  override def sendAsync[F[_] : AsyncHandler](t: T): F[MessageId] = AsyncHandler[F].send(t, producer)

  override def lastSequenceId: SequenceId = SequenceId(producer.getLastSequenceId)
  override def stats: ProducerStats = producer.getStats
  override def topic: Topic = Topic(producer.getTopic)

  override def close(): Unit = producer.close()
  override def closeAsync[F[_] : AsyncHandler]: F[Unit] = AsyncHandler[F].close(producer)
}
