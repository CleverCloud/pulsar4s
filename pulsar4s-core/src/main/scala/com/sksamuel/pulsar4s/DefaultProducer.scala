package com.sksamuel.pulsar4s

import java.util.concurrent.CompletableFuture

import org.apache.pulsar.client.api.{Producer => JProducer}
import org.apache.pulsar.client.impl.ProducerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class DefaultProducer(producer: JProducer, override val topic: Topic)
                     (implicit context: ExecutionContext) extends Producer {

  implicit def completableToFuture[T](f: CompletableFuture[T]): Future[T] = FutureConverters.toScala(f)

  override def name: ProducerName = ProducerName(producer.getProducerName)

  override def send(msg: Array[Byte]): MessageId = MessageId(producer.send(msg))

  override def sendAsync(bytes: Array[Byte]): Future[MessageId] = {
    val f = producer.sendAsync(bytes)
    f.map { id => MessageId(id) }
  }

  override def send(msg: Message): MessageId = MessageId(producer.send(Message.toJava(msg)))

  override def sendAsync(msg: Message): Future[MessageId] = {
    val f = producer.sendAsync(Message.toJava(msg))
    f.map(MessageId.apply)
  }

  override def send[T: MessageWriter](t: T): MessageId = {
    val msg = implicitly[MessageWriter[T]].write(t)
    send(msg)
  }

  override def sendAsync[T: MessageWriter](t: T): Future[MessageId] = {
    val msg = implicitly[MessageWriter[T]].write(t)
    sendAsync(msg)
  }

  override def lastSequenceId: Long = producer.getLastSequenceId

  override def stats: ProducerStats = producer.getStats

  override def close(): Unit = producer.close()
  override def closeAsync: Future[Unit] = producer.closeAsync().map(_ => ())

  override def send(msg: String): MessageId = send(msg.getBytes("UTF8"))

  override def sendAsync(msg: String): Future[MessageId] = sendAsync(msg.getBytes("UTF8"))
}
