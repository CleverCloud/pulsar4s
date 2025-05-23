package com.sksamuel.pulsar4s.zio

import java.util.concurrent.{CompletionStage, TimeUnit}
import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s.conversions.collections._
import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, ConsumerMessageWithValueTry, DefaultConsumer, DefaultProducer, DefaultReader, MessageId, Producer, TransactionContext}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{Consumer, ConsumerBuilder, ProducerBuilder, PulsarClient, Reader, ReaderBuilder, TypedMessageBuilder}
import org.apache.pulsar.client.api.transaction.Transaction
import zio.{Exit, Task, ZIO}

import scala.util.Try

class ZioAsyncHandler extends AsyncHandler[Task] {

  private def fromFuture[T](javaFutureTask: Task[CompletionStage[T]]): Task[T] =
    javaFutureTask flatMap (cs => ZIO.fromCompletionStage(cs))

  override def transform[A, B](task: Task[A])(fn: A => Try[B]): Task[B] =
    task flatMap { v => ZIO.fromTry(fn(v)) }

  override def failed(e: Throwable): Task[Nothing] =
    ZIO.fail(e)

  override def createProducer[T](builder: ProducerBuilder[T]): Task[Producer[T]] =
    fromFuture(ZIO.attempt(builder.createAsync())) flatMap (p => ZIO.attempt(new DefaultProducer(p)))

  override def createConsumer[T](builder: ConsumerBuilder[T]): Task[pulsar4s.Consumer[T]] =
    fromFuture(ZIO.attempt(builder.subscribeAsync())) flatMap (p => ZIO.attempt(new DefaultConsumer(p)))

  override def createReader[T](builder: ReaderBuilder[T]): Task[pulsar4s.Reader[T]] =
    fromFuture(ZIO.attempt(builder.createAsync())) flatMap (p => ZIO.attempt(new DefaultReader(p)))

  override def send[T](builder: TypedMessageBuilder[T]): Task[MessageId] =
    fromFuture(ZIO.attempt(builder.sendAsync())).map(MessageId.fromJava)

  override def receive[T](consumer: api.Consumer[T]): Task[ConsumerMessage[T]] =
    fromFuture(ZIO.attempt(consumer.receiveAsync())) flatMap (v => ZIO.attempt(ConsumerMessage.fromJava(v)))

  override def receiveBatch[T](consumer: Consumer[T]): Task[Vector[ConsumerMessage[T]]] =
    fromFuture(ZIO.attempt(consumer.batchReceiveAsync())) flatMap (v => ZIO
      .attempt(v.asScala.map(ConsumerMessage.fromJava).toVector))

  override def getLastMessageId[T](consumer: api.Consumer[T]): Task[MessageId] =
    fromFuture(ZIO.attempt(consumer.getLastMessageIdAsync)) flatMap (v => ZIO.attempt(MessageId.fromJava(v)))

  override def close(producer: api.Producer[_]): Task[Unit] =
    fromFuture(ZIO.attempt(producer.closeAsync())).unit

  override def close(consumer: api.Consumer[_]): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.closeAsync())).unit

  override def close(reader: Reader[_]): Task[Unit] =
    fromFuture(ZIO.attempt(reader.closeAsync())).unit

  override def close(client: PulsarClient): Task[Unit] =
    fromFuture(ZIO.attempt(client.closeAsync())).unit

  override def flush(producer: api.Producer[_]): Task[Unit] =
    fromFuture(ZIO.attempt(producer.flushAsync())).unit

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.seekAsync(messageId))).unit

  override def seekAsync(consumer: api.Consumer[_], timestamp: Long): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.seekAsync(timestamp))).unit

  override def seekAsync(reader: api.Reader[_], messageId: MessageId): Task[Unit] =
    fromFuture(ZIO.attempt(reader.seekAsync(messageId))).unit

  override def seekAsync(reader: api.Reader[_], timestamp: Long): Task[Unit] =
    fromFuture(ZIO.attempt(reader.seekAsync(timestamp))).unit

  override def nextAsync[T](reader: Reader[T]): Task[ConsumerMessage[T]] =
    fromFuture(ZIO.attempt(reader.readNextAsync())) flatMap (v => ZIO.attempt(ConsumerMessage.fromJava(v)))

  override def hasMessageAvailable(reader: Reader[_]): Task[Boolean] =
    fromFuture(ZIO.attempt(reader.hasMessageAvailableAsync)).map(identity(_))

  override def unsubscribeAsync(consumer: api.Consumer[_]): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.unsubscribeAsync())).unit

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.acknowledgeAsync(messageId))).unit

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId, txn: Transaction): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.acknowledgeAsync(messageId, txn))).unit

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.acknowledgeCumulativeAsync(messageId))).unit

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId,
                                             txn: Transaction): Task[Unit] =
    fromFuture(ZIO.attempt(consumer.acknowledgeCumulativeAsync(messageId, txn))).unit

  override def negativeAcknowledgeAsync[T](consumer: Consumer[T], messageId: MessageId): Task[Unit] =
    ZIO.attempt(consumer.negativeAcknowledge(messageId))

  override def withTransaction[E, A](
                                      builder: api.transaction.TransactionBuilder,
                                      action: TransactionContext => Task[Either[E, A]]
                                    ): Task[Either[E, A]] = {
    ZIO.acquireReleaseExitWith[Any, Throwable, TransactionContext](startTransaction(builder))(
      (txn: TransactionContext, e: Exit[Throwable, Either[E, A]]) => (txn, e) match {
        case (txn, Exit.Success(Right(_))) => txn.commit(using this).ignore
        case (txn, _) => txn.abort(using this).ignore
      }
    )(action)
  }

  override def startTransaction(builder: api.transaction.TransactionBuilder): Task[TransactionContext] =
    fromFuture(ZIO.attempt(builder.build())).map(TransactionContext(_))

  override def commitTransaction(txn: Transaction): Task[Unit] = fromFuture(ZIO.attempt(txn.commit())).unit

  override def abortTransaction(txn: Transaction): Task[Unit] = fromFuture(ZIO.attempt(txn.abort())).unit

  override def reconsumeLaterAsync[T](
                                       consumer: Consumer[T],
                                       message: ConsumerMessage[T],
                                       delayTime: Long,
                                       unit: TimeUnit
                                     ): Task[Unit] =
    ZIO
      .fromEither {
        message match {
          case consumerMessage: ConsumerMessageWithValueTry[T] =>
            Right(consumerMessage.getBaseMessage())
          case _ =>
            Left(
              new UnsupportedOperationException(
                "Only ConsumerMessageWithValueTry is supported for reconsumeLater operation"
              )
            )
        }
      }
      .map(pulsarMessage => consumer.reconsumeLater(pulsarMessage, delayTime, unit))
}

object ZioAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new ZioAsyncHandler
}
