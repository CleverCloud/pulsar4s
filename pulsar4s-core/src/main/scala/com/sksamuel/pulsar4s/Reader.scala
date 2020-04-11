package com.sksamuel.pulsar4s

import java.io.Closeable
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

trait Reader[T] extends Closeable {
  def hasMessageAvailable: Boolean
  def topic: Topic
  def next: ConsumerMessage[T]
  def next(duration: Duration): Option[ConsumerMessage[T]]
  def nextAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]]
  def closeAsync[F[_] : AsyncHandler]: F[Unit]
  def hasReachedEndOfTopic: Boolean
}

class DefaultReader[T](reader: org.apache.pulsar.client.api.Reader[T],
                       override val topic: Topic) extends Reader[T] {

  override def hasMessageAvailable: Boolean = reader.hasMessageAvailable

  override def next: ConsumerMessage[T] = ConsumerMessage.fromJava(reader.readNext)

  override def next(duration: Duration): Option[ConsumerMessage[T]] =
    Option(reader.readNext(duration.toSeconds.toInt, TimeUnit.SECONDS)).map(ConsumerMessage.fromJava)

  override def nextAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]] = implicitly[AsyncHandler[F]].nextAsync(reader)

  override def close(): Unit = reader.close()
  override def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(reader)

  /**
    * Returns true if the topic was terminated and this reader
    * has reached the end of the topic.
    */
  override def hasReachedEndOfTopic: Boolean = reader.hasReachedEndOfTopic

}
