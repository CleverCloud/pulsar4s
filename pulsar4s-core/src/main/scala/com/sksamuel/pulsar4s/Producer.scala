package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{ProducerStats, Producer => JProducer}

import scala.language.{higherKinds, implicitConversions}
import scala.util.{Failure, Success, Try}

case class ProducerName(name: String)

class Producer[T](producer: JProducer[T]) {

  def name: ProducerName = ProducerName(producer.getProducerName)

  def trySend(t: T): Try[MessageId] = Try(send(t))
  def send(t: T): MessageId = MessageId(producer.send(t))

  def sendAsync[F[_] : AsyncHandler](msg: String): F[MessageId] = sendAsync(msg.getBytes("UTF8"))
  def sendAsync[F[_] : AsyncHandler](bytes: Array[Byte]): F[MessageId] = sendAsync(Message(bytes))
  def sendAsync[F[_] : AsyncHandler](msg: Message): F[MessageId] = AsyncHandler[F].send(msg, producer)

  def sendAsync[F[+ _] : AsyncHandler](t: T): F[MessageId] = {
    implicitly[MessageWriter[T]].write(t) match {
      case Failure(e) => implicitly[AsyncHandler[F]].failed(e)
      case Success(msg) => sendAsync[F](msg)
    }
  }

  def lastSequenceId: Long = producer.getLastSequenceId
  def stats: ProducerStats = producer.getStats

  def close(): Unit = producer.close()
  def closeAsync[F[_] : AsyncHandler]: F[Unit] = AsyncHandler[F].close(producer)
}
