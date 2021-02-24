package com.sksamuel.pulsar4s.scalaz

import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, DefaultConsumer, DefaultProducer, DefaultReader, MessageId, Producer}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{Consumer, ConsumerBuilder, ProducerBuilder, PulsarClient, Reader, ReaderBuilder, TypedMessageBuilder}
import scalaz.concurrent.Task

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class ScalazAsyncHandler extends AsyncHandler[Task] {

  implicit def completableVoidToTask(f: => CompletableFuture[Void]): Task[Unit] =
    completableToTask(f).map(_ => ())

  implicit def completableToTask[T](f: => CompletableFuture[T]): Task[T] = {
    Task.async[T] { k =>
      f.whenCompleteAsync((t: T, e: Throwable) => {
        if (e != null)
          k.apply(scalaz.\/.left(e))
        else
          k.apply(scalaz.\/.right(t))
      })
    }
  }

  override def failed(e: Throwable): Task[Nothing] = Task.fail(e)

  override def createProducer[T](builder: ProducerBuilder[T]): Task[Producer[T]] =
    completableToTask(builder.createAsync()).map(new DefaultProducer(_))

  override def createConsumer[T](builder: ConsumerBuilder[T]): Task[pulsar4s.Consumer[T]] =
    completableToTask(builder.subscribeAsync()).map(new DefaultConsumer(_))

  override def createReader[T](builder: ReaderBuilder[T]): Task[pulsar4s.Reader[T]] =
    completableToTask(builder.createAsync()).map(new DefaultReader(_))

  override def send[T](t: T, producer: api.Producer[T]): Task[MessageId] =
    completableToTask(producer.sendAsync(t)).map(MessageId.fromJava)

  override def receive[T](consumer: api.Consumer[T]): Task[ConsumerMessage[T]] =
    completableToTask(consumer.receiveAsync).map(ConsumerMessage.fromJava)

  override def receiveBatch[T](consumer: Consumer[T]): Task[Vector[ConsumerMessage[T]]] =
    completableToTask(consumer.batchReceiveAsync).map(_.asScala.map(ConsumerMessage.fromJava).toVector)

  override def getLastMessageId[T](consumer: api.Consumer[T]): Task[MessageId] =
    completableToTask(consumer.getLastMessageIdAsync).map(MessageId.fromJava)

  override def unsubscribeAsync(consumer: api.Consumer[_]): Task[Unit] =
    consumer.unsubscribeAsync()

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Task[Unit] =
    consumer.seekAsync(messageId)
  
  override def seekAsync(reader: api.Reader[_], messageId: MessageId): Task[Unit] =
    reader.seekAsync(messageId)
  
  override def seekAsync(reader: api.Reader[_], timestamp: Long): Task[Unit] =
    reader.seekAsync(timestamp)

  override def transform[A, B](f: Task[A])(fn: A => Try[B]): Task[B] = f.flatMap { a =>
    fn(a) match {
      case Success(b) => Task.now(b)
      case Failure(e) => Task.fail(e)
    }
  }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    consumer.acknowledgeAsync(messageId)

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)

  override def negativeAcknowledgeAsync[T](consumer: Consumer[T], messageId: MessageId): Task[Unit] =
    Task { consumer.negativeAcknowledge(messageId) }

  override def close(reader: Reader[_]): Task[Unit] = reader.closeAsync()
  override def close(producer: api.Producer[_]): Task[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer[_]): Task[Unit] = consumer.closeAsync()
  override def close(client: PulsarClient): Task[Unit] = client.closeAsync()

  override def flush(producer: api.Producer[_]): Task[Unit] = producer.flushAsync()

  override def nextAsync[T](reader: Reader[T]): Task[ConsumerMessage[T]] =
    reader.readNextAsync().map(ConsumerMessage.fromJava)

  override def hasMessageAvailable(reader: Reader[_]): Task[Boolean] =
    reader.hasMessageAvailableAsync.map(identity(_))

  override def send[T](builder: TypedMessageBuilder[T]): Task[MessageId] =
    builder.sendAsync().map(MessageId.fromJava)
}

object ScalazAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new ScalazAsyncHandler
}
