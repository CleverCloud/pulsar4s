package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{Producer => JProducer}
import org.apache.pulsar.client.impl.ProducerStats

import scala.language.{higherKinds, implicitConversions}
import scala.util.{Failure, Success, Try}

case class ProducerName(name: String)

class Producer(producer: JProducer, val topic: Topic) {

  def name: ProducerName = ProducerName(producer.getProducerName)

  def trySend(msg: Array[Byte]): Try[MessageId] = Try(send(msg))
  def send(msg: Array[Byte]): MessageId = MessageId(producer.send(msg))

  def trySend(msg: String): Try[MessageId] = trySend(msg.getBytes("UTF8"))
  def send(msg: String): MessageId = send(msg.getBytes("UTF8"))

  def trySend(msg: Message): Try[MessageId] = Try(send(msg))
  def send(msg: Message): MessageId = MessageId(producer.send(Message.toJava(msg)))

  def trySend[T](t: T)(implicit writer: MessageWriter[T]): Try[MessageId] = writer.write(t).flatMap(trySend)
  def send[T](t: T)(implicit writer: MessageWriter[T]): MessageId = {
    writer.write(t) match {
      case Failure(e) => throw e
      case Success(msg) => send(msg)
    }
  }

  def sendAsync[F[_] : AsyncHandler](msg: String): F[MessageId] = sendAsync(msg.getBytes("UTF8"))
  def sendAsync[F[_] : AsyncHandler](bytes: Array[Byte]): F[MessageId] = sendAsync(Message(bytes))
  def sendAsync[F[_] : AsyncHandler](msg: Message): F[MessageId] = AsyncHandler[F].send(msg, producer)

  def sendAsync[T: MessageWriter, F[+ _] : AsyncHandler](t: T): F[MessageId] = {
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
