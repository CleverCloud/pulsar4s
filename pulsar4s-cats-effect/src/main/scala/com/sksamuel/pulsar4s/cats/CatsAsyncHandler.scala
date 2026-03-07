package com.sksamuel.pulsar4s.cats

import cats.effect.Resource.ExitCase
import cats.effect.{Async, IO, Resource}
import cats.implicits._
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s._
import com.sksamuel.pulsar4s.conversions.collections._
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.transaction.Transaction
import org.apache.pulsar.client.api.{Consumer => _, MessageId => _, Producer => _, PulsarClient => _, Reader => _, _}

import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.util.{Failure, Success, Try}


object CatsAsyncHandler extends CatsAsyncHandlerLowPriority {
  implicit def handler: AsyncHandler[IO] = asyncHandlerForCatsEffectAsync[IO]
}

trait CatsAsyncHandlerLowPriority {

  private def liftF[F[_] : Async, T](f: F[CompletableFuture[T]]): F[T] =
    Async[F].fromCompletableFuture(f)

  implicit def asyncHandlerForCatsEffectAsync[F[_] : Async]: AsyncHandler[F] = new AsyncHandler[F] with Logging {

    override def failed(e: Throwable): F[Nothing] = Async[F].raiseError(e)

    override def createProducer[T](builder: ProducerBuilder[T]): F[Producer[T]] =
      liftF(Async[F].delay(builder.createAsync())).map(new DefaultProducer(_))

    override def createConsumer[T](builder: ConsumerBuilder[T]): F[Consumer[T]] = {
      logger.debug("Create consumer async... for builder. ")
      liftF(Async[F].delay(builder.subscribeAsync())).map(new DefaultConsumer(_))
    }

    override def createReader[T](builder: ReaderBuilder[T]): F[pulsar4s.Reader[T]] =
      liftF(Async[F].delay(builder.createAsync())).map(new DefaultReader(_))

    override def send[T](t: T, producer: api.Producer[T]): F[MessageId] =
      liftF(Async[F].delay(producer.sendAsync(t))).map(MessageId.fromJava)

    override def receive[T](consumer: api.Consumer[T]): F[ConsumerMessage[T]] =
      liftF(Async[F].delay(consumer.receiveAsync())).map(ConsumerMessage.fromJava)

    override def receiveBatch[T](consumer: api.Consumer[T]): F[Vector[ConsumerMessage[T]]] =
      liftF(Async[F].delay(consumer.batchReceiveAsync())).map(_.asScala.map(ConsumerMessage.fromJava).toVector)

    override def unsubscribeAsync(consumer: api.Consumer[_]): F[Unit] =
      liftF(Async[F].delay(consumer.unsubscribeAsync())).void

    override def getLastMessageId[T](consumer: api.Consumer[T]): F[MessageId] =
      liftF(Async[F].delay(consumer.getLastMessageIdAsync)).map(MessageId.fromJava)

    override def close(producer: api.Producer[_]): F[Unit] =
      liftF(Async[F].delay(producer.closeAsync())).void

    override def close(consumer: api.Consumer[_]): F[Unit] =
      liftF(Async[F].delay(consumer.closeAsync())).void

    override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): F[Unit] =
      liftF(Async[F].delay(consumer.seekAsync(messageId))).void

    override def seekAsync(consumer: JConsumer[_], timestamp: Long): F[Unit] =
      liftF(Async[F].delay(consumer.seekAsync(timestamp))).void

    override def seekAsync(reader: api.Reader[_], messageId: MessageId): F[Unit] =
      liftF(Async[F].delay(reader.seekAsync(messageId))).void

    override def seekAsync(reader: api.Reader[_], timestamp: Long): F[Unit] =
      liftF(Async[F].delay(reader.seekAsync(timestamp))).void

    override def transform[A, B](t: F[A])(fn: A => Try[B]): F[B] =
      t.flatMap { a =>
        fn(a) match {
          case Success(b) => Async[F].pure(b)
          case Failure(e) => Async[F].raiseError(e)
        }
      }

    override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      liftF(Async[F].delay(consumer.acknowledgeAsync(messageId))).void

    override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId, txn: Transaction): F[Unit] =
      liftF(Async[F].delay(consumer.acknowledgeAsync(messageId, txn))).void

    override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      liftF(Async[F].delay(consumer.acknowledgeCumulativeAsync(messageId))).void

    override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId,
                                               txn: Transaction): F[Unit] =
      liftF(Async[F].delay(consumer.acknowledgeCumulativeAsync(messageId, txn))).void

    override def negativeAcknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      Async[F].delay {
        consumer.negativeAcknowledge(messageId)
      }

    override def close(reader: api.Reader[_]): F[Unit] =
      liftF(Async[F].delay(reader.closeAsync())).void

    override def flush(producer: api.Producer[_]): F[Unit] =
      liftF(Async[F].delay(producer.flushAsync())).void

    override def close(client: api.PulsarClient): F[Unit] =
      liftF(Async[F].delay(client.closeAsync())).void

    override def nextAsync[T](reader: api.Reader[T]): F[ConsumerMessage[T]] =
      liftF(Async[F].delay(reader.readNextAsync())).map(ConsumerMessage.fromJava)

    override def hasMessageAvailable(reader: api.Reader[_]): F[Boolean] =
      liftF(Async[F].delay(reader.hasMessageAvailableAsync)).map(identity(_))

    override def send[T](builder: TypedMessageBuilder[T]): F[MessageId] =
      liftF(Async[F].delay(builder.sendAsync())).map(MessageId.fromJava)

    override def withTransaction[E, A](
                                        builder: api.transaction.TransactionBuilder,
                                        action: TransactionContext => F[Either[E, A]]
                                      ): F[Either[E, A]] = {
      Resource.makeCase(startTransaction(builder)) { (txn, exitCase) =>
        if (exitCase == ExitCase.Succeeded) Async[F].unit else txn.abort
      }.use { txn =>
        action(txn).flatMap { result =>
          (if (result.isRight) txn.commit else txn.abort).map(_ => result)
        }
      }
    }

    def startTransaction(builder: api.transaction.TransactionBuilder): F[TransactionContext] =
      liftF(Async[F].delay(builder.build())).map(TransactionContext(_))

    def commitTransaction(txn: Transaction): F[Unit] = liftF(Async[F].delay(txn.commit())).void

    def abortTransaction(txn: Transaction): F[Unit] = liftF(Async[F].delay(txn.abort())).void

    override def reconsumeLaterAsync[T](
                                         consumer: JConsumer[T],
                                         message: ConsumerMessage[T],
                                         delayTime: Long,
                                         unit: TimeUnit
                                       ): F[Unit] = {
      Async[F]
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
        .flatMap(pulsarMessage => Async[F].delay(consumer.reconsumeLater(pulsarMessage, delayTime, unit)))
    }
  }

}
