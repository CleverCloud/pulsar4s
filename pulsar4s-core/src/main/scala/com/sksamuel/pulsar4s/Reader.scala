package com.sksamuel.pulsar4s

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.language.higherKinds

class Reader[T](reader: org.apache.pulsar.client.api.Reader[T],
                val topic: Topic) {

  def next: ConsumerMessage[T] = ConsumerMessage.fromJava(reader.readNext)

  def next(duration: Duration): Option[ConsumerMessage[T]] =
    Option(reader.readNext(duration.toSeconds.toInt, TimeUnit.SECONDS)).map(ConsumerMessage.fromJava)

  def nextAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]] = implicitly[AsyncHandler[F]].nextAsync(reader)

  def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(reader)

  /**
    * Returns true if the topic was terminated and this reader
    * has reached the end of the topic.
    */
  def hasReachedEndOfTopic: Boolean = reader.hasReachedEndOfTopic
}
