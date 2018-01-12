package com.sksamuel.pulsar4s

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.language.higherKinds

class Reader(reader: org.apache.pulsar.client.api.Reader,
             val topic: Topic,
             val subscription: Subscription) {

  def next: Message = reader.readNext()

  def next(duration: Duration): Message = reader.readNext(duration.toSeconds.toInt, TimeUnit.SECONDS)

  def nextAsync[F[_] : AsyncHandler]: F[Message] = implicitly[AsyncHandler[F]].nextAsync(reader)

  def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(reader)

  def hasReachedEndOfTopic: Boolean = reader.hasReachedEndOfTopic
}
