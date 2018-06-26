package com.sksamuel.pulsar4s

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.language.higherKinds

class Reader[T](reader: org.apache.pulsar.client.api.Reader[T],
                val topic: Topic) {

  def next: Message[T] = reader.readNext()

  def next(duration: Duration): Message[T] = reader.readNext(duration.toSeconds.toInt, TimeUnit.SECONDS)

  def nextAsync[F[_] : AsyncHandler]: F[Message[T]] = implicitly[AsyncHandler[F]].nextAsync(reader)

  def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(reader)

  def hasReachedEndOfTopic: Boolean = reader.hasReachedEndOfTopic
}
