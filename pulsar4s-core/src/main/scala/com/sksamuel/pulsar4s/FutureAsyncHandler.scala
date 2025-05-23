package com.sksamuel.pulsar4s

import com.sksamuel.pulsar4s.conversions.collections._
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.transaction.Transaction
import org.apache.pulsar.client.api.{ConsumerBuilder, ReaderBuilder, TypedMessageBuilder}

import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.compat.java8.FutureConverters
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class FutureAsyncHandler(implicit ec: ExecutionContext) extends AsyncHandler[Future] {

  implicit class VoidCompletableFutureOps(val completableFuture: CompletableFuture[Void]) {
    def toScala: Future[Unit] = new CompletionStageOps(completableFuture).toScala.map(_ => ())
  }

  override def failed(e: Throwable): Future[Nothing] = Future.failed(e)

  override def createProducer[T](builder: api.ProducerBuilder[T]): Future[Producer[T]] = {
    builder.createAsync().thenApply[Producer[T]](new DefaultProducer(_)).toScala
  }

  override def createConsumer[T](builder: ConsumerBuilder[T]): Future[Consumer[T]] =
    builder.subscribeAsync().thenApply[Consumer[T]](new DefaultConsumer(_)).toScala

  override def createReader[T](builder: ReaderBuilder[T]): Future[Reader[T]] =
    builder.createAsync().thenApply[Reader[T]](new DefaultReader(_)).toScala

  override def receive[T](consumer: api.Consumer[T]): Future[ConsumerMessage[T]] = {
    val future = consumer.receiveAsync()
    FutureConverters.toScala(future).map(ConsumerMessage.fromJava)
  }

  override def receiveBatch[T](consumer: JConsumer[T]): Future[Vector[ConsumerMessage[T]]] =
    consumer.batchReceiveAsync().toScala.map(_.asScala.map(ConsumerMessage.fromJava).toVector)

  override def unsubscribeAsync(consumer: api.Consumer[_]): Future[Unit] = consumer.unsubscribeAsync().toScala

  override def getLastMessageId[T](consumer: api.Consumer[T]): Future[MessageId] = {
    val future = consumer.getLastMessageIdAsync
    FutureConverters.toScala(future).map(MessageId.fromJava)
  }

  override def close(producer: api.Producer[_]): Future[Unit] = producer.closeAsync().toScala

  override def close(consumer: api.Consumer[_]): Future[Unit] = consumer.closeAsync().toScala

  override def close(client: api.PulsarClient): Future[Unit] = client.closeAsync().toScala

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Future[Unit] =
    consumer.seekAsync(messageId).toScala

  override def seekAsync(consumer: api.Consumer[_], timestamp: Long): Future[Unit] =
    consumer.seekAsync(timestamp).toScala

  override def seekAsync(reader: api.Reader[_], messageId: MessageId): Future[Unit] =
    reader.seekAsync(messageId).toScala

  override def seekAsync(reader: api.Reader[_], timestamp: Long): Future[Unit] =
    reader.seekAsync(timestamp).toScala

  override def transform[A, B](f: Future[A])(fn: A => Try[B]): Future[B] = f.flatMap { a =>
    fn(a) match {
      case Success(b) => Future.successful(b)
      case Failure(e) => Future.failed(e)
    }
  }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Future[Unit] =
    consumer.acknowledgeAsync(messageId).toScala

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId, txn: Transaction): Future[Unit] =
    consumer.acknowledgeAsync(messageId, txn).toScala

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Future[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId).toScala

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId,
                                             txn: Transaction): Future[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId, txn).toScala

  override def negativeAcknowledgeAsync[T](consumer: JConsumer[T], messageId: MessageId): Future[Unit] =
    Future.successful(consumer.negativeAcknowledge(messageId))

  override def reconsumeLaterAsync[T](
                                       consumer: api.Consumer[T],
                                       message: ConsumerMessage[T],
                                       delayTime: Long,
                                       unit: TimeUnit
                                     ): Future[Unit] =
    message match {
      case consumerMessage: ConsumerMessageWithValueTry[T] =>
        Future.successful(consumer.reconsumeLater(consumerMessage.getBaseMessage(), delayTime, unit))
      case _ =>
        Future.failed(
          new UnsupportedOperationException(
            "Only ConsumerMessageWithValueTry is supported for reconsumeLater operation"
          )
        )
    }


  override def close(reader: api.Reader[_]): Future[Unit] = reader.closeAsync().toScala

  override def flush(producer: api.Producer[_]): Future[Unit] = producer.flushAsync().toScala

  override def nextAsync[T](reader: api.Reader[T]): Future[ConsumerMessage[T]] =
    reader.readNextAsync().toScala.map(ConsumerMessage.fromJava)

  override def hasMessageAvailable(reader: api.Reader[_]): Future[Boolean] =
    reader.hasMessageAvailableAsync.toScala.map(identity(_))

  override def send[T](builder: TypedMessageBuilder[T]): Future[MessageId] =
    builder.sendAsync().toScala.map(MessageId.fromJava)

  override def withTransaction[E, A](
                                      builder: api.transaction.TransactionBuilder,
                                      action: TransactionContext => Future[Either[E, A]]
                                    ): Future[Either[E, A]] = {
    startTransaction(builder).flatMap { txn =>
      Future.unit.flatMap(_ => action(txn)).transformWith {
        case Success(Right(value)) =>
          txn.commit.transform(_ => Success(Right(value)))
        case result =>
          txn.abort.transform(_ => result)
      }
    }
  }

  override def startTransaction(builder: api.transaction.TransactionBuilder): Future[TransactionContext] =
    builder.build().toScala.map(TransactionContext(_))

  override def commitTransaction(txn: Transaction): Future[Unit] = txn.commit().toScala.map(_ => ())

  override def abortTransaction(txn: Transaction): Future[Unit] = txn.abort().toScala.map(_ => ())

}
