package com.sksamuel.pulsar4s.zio

import java.util.concurrent.CompletionStage

import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, DefaultProducer, MessageId, Producer}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{Consumer, Reader, TypedMessageBuilder}
import org.apache.pulsar.client.api.ProducerBuilder
import zio.interop.javaz._
import zio.{Task, UIO}

import scala.util.Try

class ZioAsyncHandler extends AsyncHandler[Task] {

  private def fromFuture[T](javaFutureUIO: UIO[CompletionStage[T]]): Task[T] =
    javaFutureUIO.toZio

  override def transform[A, B](t: Task[A])(fn: A => Try[B]): Task[B] =
    t >>= { v => Task.fromTry(fn(v)) }

  override def failed(e: Throwable): Task[Nothing] =
    Task.fail(e)

  override def createProducer[T](builder: ProducerBuilder[T]): zio.Task[Producer[T]] = {
    fromFuture(UIO(builder.createAsync())).map(new DefaultProducer(_))
  }

  override def send[T](t: T, producer: api.Producer[T]): Task[MessageId] =
    fromFuture(UIO(producer.sendAsync(t))).map(MessageId.fromJava)

  override def send[T](builder: TypedMessageBuilder[T]): Task[MessageId] =
    fromFuture(UIO(builder.sendAsync())).map(MessageId.fromJava)

  override def receive[T](consumer: api.Consumer[T]): Task[ConsumerMessage[T]] =
    fromFuture(UIO(consumer.receiveAsync())) >>= (v => Task(ConsumerMessage.fromJava(v)))

  override def close(producer: api.Producer[_]): Task[Unit] =
    fromFuture(UIO(producer.closeAsync())).unit

  override def close(consumer: api.Consumer[_]): Task[Unit] =
    fromFuture(UIO(consumer.closeAsync())).unit

  override def close(reader: Reader[_]): Task[Unit] =
    fromFuture(UIO(reader.closeAsync())).unit

  override def flush(producer: api.Producer[_]): Task[Unit] =
    fromFuture(UIO(producer.flushAsync())).unit

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Task[Unit] =
    fromFuture(UIO(consumer.seekAsync(messageId))).unit

  override def nextAsync[T](reader: Reader[T]): Task[ConsumerMessage[T]] =
    fromFuture(UIO(reader.readNextAsync())) >>= (v => Task(ConsumerMessage.fromJava(v)))

  override def unsubscribeAsync(consumer: api.Consumer[_]): Task[Unit] =
    fromFuture(UIO(consumer.unsubscribeAsync())).unit

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    fromFuture(UIO(consumer.acknowledgeAsync(messageId))).unit

  override def negativeAcknowledgeAsync[T](consumer: Consumer[T], messageId: MessageId): Task[Unit] =
    Task(consumer.negativeAcknowledge(messageId))

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    fromFuture(UIO(consumer.acknowledgeCumulativeAsync(messageId))).unit

}

object ZioAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new ZioAsyncHandler
}
