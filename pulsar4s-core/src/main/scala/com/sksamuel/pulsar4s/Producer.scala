package com.sksamuel.pulsar4s

import java.util.concurrent.CompletableFuture

import org.apache.pulsar.client.api.{Producer => JProducer}
import org.apache.pulsar.client.impl.ProducerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try

case class ProducerName(name: String)

class Producer(producer: JProducer, val topic: Topic)
              (implicit context: ExecutionContext) {

  implicit def completableToFuture[U](f: CompletableFuture[U]): Future[U] = FutureConverters.toScala(f)
  implicit def voidCompletableToFuture(f: CompletableFuture[Void]): Future[Unit] = f.map(_ => ())

  def name: ProducerName = ProducerName(producer.getProducerName)

  def send(msg: Array[Byte]): Either[Throwable, MessageId] = Try {
    MessageId(producer.send(msg))
  }.toEither

  def sendAsync(bytes: Array[Byte]): Future[MessageId] = {
    val f = producer.sendAsync(bytes)
    f.map(MessageId.apply)
  }

  def send(msg: Message): Either[Throwable, MessageId] = Try {
    MessageId(producer.send(Message.toJava(msg)))
  }.toEither

  def sendAsync(msg: Message): Future[MessageId] = {
    val f = producer.sendAsync(Message.toJava(msg))
    f.map(MessageId.apply)
  }

  def send[T: MessageWriter](t: T): Either[Throwable, MessageId] = {
    val msg = implicitly[MessageWriter[T]].write(t)
    msg match {
      case Left(e) => Left(e)
      case Right(message) => send(message)
    }
  }

  def sendAsync[T: MessageWriter](t: T): Future[MessageId] = {
    val msg = implicitly[MessageWriter[T]].write(t)
    msg match {
      case Left(e) => Future.failed(e)
      case Right(message) => sendAsync(message)
    }
  }

  def lastSequenceId: Long = producer.getLastSequenceId

  def stats: ProducerStats = producer.getStats

  def close(): Unit = producer.close()
  def closeAsync: Future[Unit] = producer.closeAsync().map(_ => ())

  def send(msg: String): Either[Throwable, MessageId] = send(msg.getBytes("UTF8"))

  def sendAsync(msg: String): Future[MessageId] = sendAsync(msg.getBytes("UTF8"))
}
