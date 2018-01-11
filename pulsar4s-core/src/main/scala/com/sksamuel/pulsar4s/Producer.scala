package com.sksamuel.pulsar4s

import java.util.concurrent.CompletableFuture

import org.apache.pulsar.client.api.{Producer => JProducer}
import org.apache.pulsar.client.impl.ProducerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

case class ProducerName(name: String)

class Producer(producer: JProducer, val topic: Topic)
              (implicit context: ExecutionContext) {

  implicit def completableToFuture[U](f: CompletableFuture[U]): Future[U] = FutureConverters.toScala(f)
  implicit def voidCompletableToFuture(f: CompletableFuture[Void]): Future[Unit] = f.map(_ => ())

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

  def sendAsync(msg: String): Future[MessageId] = sendAsync(msg.getBytes("UTF8"))
  def sendAsync(bytes: Array[Byte]): Future[MessageId] = {
    val f = producer.sendAsync(bytes)
    f.map(MessageId.apply)
  }
  def sendAsync(msg: Message): Future[MessageId] = {
    val f = producer.sendAsync(Message.toJava(msg))
    f.map(MessageId.apply)
  }
  def sendAsync[T](t: T)(implicit writer: MessageWriter[T]): Future[MessageId] = {
    writer.write(t) match {
      case Failure(e) => Future.failed(e)
      case Success(msg) => sendAsync(msg)
    }
  }

  def lastSequenceId: Long = producer.getLastSequenceId
  def stats: ProducerStats = producer.getStats

  def close(): Unit = producer.close()
  def closeAsync: Future[Unit] = producer.closeAsync().map(_ => ())
}
