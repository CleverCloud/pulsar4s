package com.sksamuel.pulsar4s

import java.io.Closeable
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

trait Reader[T] extends Closeable {
  def hasMessageAvailable: Boolean
  def hasMessageAvailableAsync[F[_]: AsyncHandler]: F[Boolean]
  def topic: Topic
  def next: ConsumerMessage[T]
  def next(duration: Duration): Option[ConsumerMessage[T]]
  def nextAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]]
  def isConnected: Boolean
  def closeAsync[F[_] : AsyncHandler]: F[Unit]
  def seek(timestamp: Long): Unit
  def seek(messageId: MessageId): Unit
  def seekAsync[F[_] : AsyncHandler](timestamp: Long): F[Unit]
  def seekAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] 
  def hasReachedEndOfTopic: Boolean
}

class DefaultReader[T](reader: org.apache.pulsar.client.api.Reader[T]) extends Reader[T] {
  override def hasMessageAvailable: Boolean = reader.hasMessageAvailable

  override def hasMessageAvailableAsync[F[_] : AsyncHandler]: F[Boolean] = implicitly[AsyncHandler[F]].hasMessageAvailable(reader)

  override lazy val topic: Topic = Topic(reader.getTopic)

  override def next: ConsumerMessage[T] = ConsumerMessage.fromJava(reader.readNext)

  override def next(duration: Duration): Option[ConsumerMessage[T]] =
    Option(reader.readNext(duration.toSeconds.toInt, TimeUnit.SECONDS)).map(ConsumerMessage.fromJava)

  override def nextAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]] = implicitly[AsyncHandler[F]].nextAsync(reader)

  override def isConnected: Boolean = reader.isConnected

  override def close(): Unit = reader.close()
  override def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(reader)

  override def seek(timestamp: Long): Unit = reader.seek(timestamp)
  override def seek(messageId: MessageId): Unit = reader.seek(messageId)
  override def seekAsync[F[_] : AsyncHandler](timestamp: Long): F[Unit] = implicitly[AsyncHandler[F]].seekAsync(reader, timestamp)
  override def seekAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] = implicitly[AsyncHandler[F]].seekAsync(reader, messageId)

  /**
    * Returns true if the topic was terminated and this reader
    * has reached the end of the topic.
    */
  override def hasReachedEndOfTopic: Boolean = reader.hasReachedEndOfTopic

}
