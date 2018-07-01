package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{ProducerStats, Schema, Producer => JProducer}

import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

case class ProducerName(name: String)

class Producer[T](producer: JProducer[T])(implicit schema: Schema[T]) {

  def name: ProducerName = ProducerName(producer.getProducerName)

  def send(t: T): MessageId = MessageId(producer.send(t))
  def sendAsync[F[_] : AsyncHandler](t: T): F[MessageId] = AsyncHandler[F].send(t, producer)
  def trySend(t: T): Try[MessageId] = Try(send(t))

  def lastSequenceId: Long = producer.getLastSequenceId
  def stats: ProducerStats = producer.getStats
  def topic: Topic = Topic(producer.getTopic)

  def close(): Unit = producer.close()
  def closeAsync[F[_] : AsyncHandler]: F[Unit] = AsyncHandler[F].close(producer)
}
