package com.sksamuel.pulsar4s.zio

import java.util.concurrent.CompletionStage

import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, DefaultConsumer, DefaultProducer, DefaultReader, MessageId, Producer, TransactionContext}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{Consumer, ConsumerBuilder, ProducerBuilder, PulsarClient, Reader, ReaderBuilder, TypedMessageBuilder}
import org.apache.pulsar.client.api.transaction.Transaction
import zio.{Exit, Task, ZIO}

import scala.collection.JavaConverters._
import scala.util.Try

class ZioAsyncHandler extends AsyncHandler[Task] {

  private def fromFuture[T](javaFutureTask: Task[CompletionStage[T]]): Task[T] =
    javaFutureTask >>= (cs => ZIO.fromCompletionStage(cs))

  override def transform[A, B](task: Task[A])(fn: A => Try[B]): Task[B] =
    task >>= { v => Task.fromTry(fn(v)) }

  override def failed(e: Throwable): Task[Nothing] =
    Task.fail(e)

  override def createProducer[T](builder: ProducerBuilder[T]): Task[Producer[T]] =
    fromFuture(Task(builder.createAsync())) >>= (p => Task(new DefaultProducer(p)))

  override def createConsumer[T](builder: ConsumerBuilder[T]): Task[pulsar4s.Consumer[T]] =
    fromFuture(Task(builder.subscribeAsync())) >>= (p => Task(new DefaultConsumer(p)))

  override def createReader[T](builder: ReaderBuilder[T]): Task[pulsar4s.Reader[T]] =
    fromFuture(Task(builder.createAsync())) >>= (p => Task(new DefaultReader(p)))

  override def send[T](builder: TypedMessageBuilder[T]): Task[MessageId] =
    fromFuture(Task(builder.sendAsync())).map(MessageId.fromJava)

  override def receive[T](consumer: api.Consumer[T]): Task[ConsumerMessage[T]] =
    fromFuture(Task(consumer.receiveAsync())) >>= (v => Task(ConsumerMessage.fromJava(v)))

  override def receiveBatch[T](consumer: Consumer[T]): Task[Vector[ConsumerMessage[T]]] =
    fromFuture(Task(consumer.batchReceiveAsync())) >>= (v => Task(v.asScala.map(ConsumerMessage.fromJava).toVector))

  override def getLastMessageId[T](consumer: api.Consumer[T]): Task[MessageId] =
    fromFuture(Task(consumer.getLastMessageIdAsync)) >>= (v => Task(MessageId.fromJava(v)))

  override def close(producer: api.Producer[_]): Task[Unit] =
    fromFuture(Task(producer.closeAsync())).unit

  override def close(consumer: api.Consumer[_]): Task[Unit] =
    fromFuture(Task(consumer.closeAsync())).unit

  override def close(reader: Reader[_]): Task[Unit] =
    fromFuture(Task(reader.closeAsync())).unit

  override def close(client: PulsarClient): Task[Unit] =
    fromFuture(Task(client.closeAsync())).unit

  override def flush(producer: api.Producer[_]): Task[Unit] =
    fromFuture(Task(producer.flushAsync())).unit

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Task[Unit] =
    fromFuture(Task(consumer.seekAsync(messageId))).unit

  override def seekAsync(reader: api.Reader[_], messageId: MessageId): Task[Unit] =
    fromFuture(Task(reader.seekAsync(messageId))).unit

  override def seekAsync(reader: api.Reader[_], timestamp: Long): Task[Unit] =
    fromFuture(Task(reader.seekAsync(timestamp))).unit

  override def nextAsync[T](reader: Reader[T]): Task[ConsumerMessage[T]] =
    fromFuture(Task(reader.readNextAsync())) >>= (v => Task(ConsumerMessage.fromJava(v)))

  override def hasMessageAvailable(reader: Reader[_]): Task[Boolean] =
    fromFuture(Task(reader.hasMessageAvailableAsync)).map(identity(_))

  override def unsubscribeAsync(consumer: api.Consumer[_]): Task[Unit] =
    fromFuture(Task(consumer.unsubscribeAsync())).unit

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    fromFuture(Task(consumer.acknowledgeAsync(messageId))).unit

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId, txn: Transaction): Task[Unit] =
    fromFuture(Task(consumer.acknowledgeAsync(messageId, txn))).unit

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    fromFuture(Task(consumer.acknowledgeCumulativeAsync(messageId))).unit
    
  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId, txn: Transaction): Task[Unit] =
    fromFuture(Task(consumer.acknowledgeCumulativeAsync(messageId, txn))).unit
    
  override def negativeAcknowledgeAsync[T](consumer: Consumer[T], messageId: MessageId): Task[Unit] =
    Task(consumer.negativeAcknowledge(messageId))

  override def withTransaction[E, A](
    builder: api.transaction.TransactionBuilder,
    action: TransactionContext => Task[Either[E, A]]
  ): Task[Either[E, A]] = {
    Task.bracketExit[TransactionContext, Either[E, A]](
      acquire = startTransaction(builder),
      release = {
        case (txn, Exit.Success(Right(_))) => txn.commit(this).ignore
        case (txn, _) => txn.abort(this).ignore
      },
      use = action
    )
  }

  override def startTransaction(builder: api.transaction.TransactionBuilder): Task[TransactionContext] =
    fromFuture(Task(builder.build())).map(TransactionContext(_))
  override def commitTransaction(txn: Transaction): Task[Unit] = fromFuture(Task(txn.commit())).unit
  override def abortTransaction(txn: Transaction): Task[Unit] = fromFuture(Task(txn.abort())).unit
}

object ZioAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new ZioAsyncHandler
}
