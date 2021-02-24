package com.sksamuel.pulsar4s.monixs

import java.util.concurrent.CompletableFuture

import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, DefaultConsumer, DefaultProducer, DefaultReader, MessageId, Producer}
import monix.eval.Task
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{Consumer, ConsumerBuilder, ProducerBuilder, PulsarClient, Reader, ReaderBuilder, TypedMessageBuilder}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.compat.java8.FutureConverters
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class MonixAsyncHandler extends AsyncHandler[Task] {

  implicit def completableTToFuture[T](f: => CompletableFuture[T]): Future[T] =
    FutureConverters.toScala(f)

  implicit def completableVoidToTask(f: => CompletableFuture[Void]): Task[Unit] =
    Task.deferFuture(FutureConverters.toScala(f)).map(_ => ())

  override def failed(e: Throwable): Task[Nothing] = Task.raiseError(e)

  override def createProducer[T](builder: ProducerBuilder[T]): Task[Producer[T]] =
    Task.deferFuture(FutureConverters.toScala(builder.createAsync())).map(new DefaultProducer(_))

  override def createConsumer[T](builder: ConsumerBuilder[T]): Task[pulsar4s.Consumer[T]] =
    Task.deferFuture(FutureConverters.toScala(builder.subscribeAsync())).map(new DefaultConsumer(_))

  override def createReader[T](builder: ReaderBuilder[T]): Task[pulsar4s.Reader[T]] =
    Task.deferFuture(FutureConverters.toScala(builder.createAsync())).map(new DefaultReader(_))

  override def send[T](t: T, producer: api.Producer[T]): Task[MessageId] = {
    Task.deferFuture {
      val future = producer.sendAsync(t)
      FutureConverters.toScala(future)
    }.map { id => MessageId.fromJava(id) }
  }

  override def receive[T](consumer: api.Consumer[T]): Task[ConsumerMessage[T]] = {
    Task.deferFuture {
      val future = consumer.receiveAsync()
      FutureConverters.toScala(future)
    }.map(ConsumerMessage.fromJava)
  }

  override def receiveBatch[T](consumer: Consumer[T]): Task[Vector[ConsumerMessage[T]]] =
    Task.deferFuture {
      FutureConverters.toScala(consumer.batchReceiveAsync())
    }.map(_.asScala.map(ConsumerMessage.fromJava).toVector)

  override def getLastMessageId[T](consumer: api.Consumer[T]): Task[MessageId] = {
    Task.deferFuture {
      val future = consumer.getLastMessageIdAsync
      FutureConverters.toScala(future)
    }.map(MessageId.fromJava)
  }

  def unsubscribeAsync(consumer: api.Consumer[_]): Task[Unit] = consumer.unsubscribeAsync()

  override def close(producer: api.Producer[_]): Task[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer[_]): Task[Unit] = consumer.closeAsync()

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Task[Unit] =
    consumer.seekAsync(messageId)
  
  override def seekAsync(reader: api.Reader[_], messageId: MessageId): Task[Unit] =
    reader.seekAsync(messageId)
  
  override def seekAsync(reader: api.Reader[_], timestamp: Long): Task[Unit] =
    reader.seekAsync(timestamp)


  override def transform[A, B](t: Task[A])(fn: A => Try[B]): Task[B] =
    t.flatMap { a =>
      fn(a) match {
        case Success(b) => Task.now(b)
        case Failure(e) => Task.raiseError(e)
      }
    }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    consumer.acknowledgeAsync(messageId)

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)

  override def negativeAcknowledgeAsync[T](consumer: Consumer[T], messageId: MessageId): Task[Unit] =
    Task { consumer.negativeAcknowledge(messageId) }

  override def close(reader: Reader[_]): Task[Unit] = reader.closeAsync()
  override def close(client: PulsarClient): Task[Unit] = client.closeAsync()

  override def flush(producer: api.Producer[_]): Task[Unit] = producer.flushAsync()

  override def nextAsync[T](reader: Reader[T]): Task[ConsumerMessage[T]] =
    Task.deferFuture(reader.readNextAsync()).map(ConsumerMessage.fromJava)

  override def hasMessageAvailable(reader: Reader[_]): Task[Boolean] =
    Task.deferFuture(reader.hasMessageAvailableAsync)

  override def send[T](builder: TypedMessageBuilder[T]): Task[MessageId] =
    Task.deferFuture(builder.sendAsync()).map(MessageId.fromJava)
}

object MonixAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new MonixAsyncHandler
}
