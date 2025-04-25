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

import java.util.concurrent._
import scala.concurrent.ExecutionException
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


object CatsAsyncHandler extends CatsAsyncHandlerLowPriority {
  implicit def handler: AsyncHandler[IO] = asyncHandlerForCatsEffectAsync[IO]
}

trait CatsAsyncHandlerLowPriority {

  object CompletableFutureConverters extends Logging {

    implicit class CompletableOps[F[_] : Async, T](f: => F[CompletableFuture[T]]) {
      def liftF: F[T] = {
        f.flatMap { f =>
          Async[F].defer {
            if (f.isDone) {
              try {
                Async[F].pure(f.get())
              } catch {
                case e: CompletionException =>
                  Async[F].raiseError(e.getCause)
                case e: ExecutionException =>
                  Async[F].raiseError(e.getCause)
                case NonFatal(e) =>
                  Async[F].raiseError(e)
              }
            } else {
              Async[F].async_[T] { cb =>
                f.handle[Unit] { (res: T, err: Throwable) =>
                  err match {
                    case null =>
                      cb(Right(res))
                    case _: CancellationException =>
                      ()
                    case ex: CompletionException if ex.getCause ne null =>
                      cb(Left(ex.getCause))
                    case ex =>
                      cb(Left(ex))
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  implicit def asyncHandlerForCatsEffectAsync[F[_] : Async]: AsyncHandler[F] = new AsyncHandler[F] with Logging {

    import CompletableFutureConverters._

    override def failed(e: Throwable): F[Nothing] = Async[F].raiseError(e)

    override def createProducer[T](builder: ProducerBuilder[T]): F[Producer[T]] = Async[F].delay {
      builder.createAsync()
    }.liftF.map(new DefaultProducer(_))

    override def createConsumer[T](builder: ConsumerBuilder[T]): F[Consumer[T]] = Async[F].delay {
      logger.debug("Create consumer async... for builder. ")
      builder.subscribeAsync()
    }.liftF.map(new DefaultConsumer(_))

    override def createReader[T](builder: ReaderBuilder[T]): F[pulsar4s.Reader[T]] = Async[F].delay {
      builder.createAsync()
    }.liftF.map(new DefaultReader(_))

    override def send[T](t: T, producer: api.Producer[T]): F[MessageId] = Async[F].delay {
      producer.sendAsync(t)
    }.liftF.map(MessageId.fromJava)

    override def receive[T](consumer: api.Consumer[T]): F[ConsumerMessage[T]] = Async[F].delay {
      consumer.receiveAsync()
    }.liftF.map(ConsumerMessage.fromJava)

    override def receiveBatch[T](consumer: api.Consumer[T]): F[Vector[ConsumerMessage[T]]] =
      Async[F].delay {
        consumer.batchReceiveAsync()
      }.liftF.map(_.asScala.map(ConsumerMessage.fromJava).toVector)

    override def unsubscribeAsync(consumer: api.Consumer[_]): F[Unit] = Async[F].delay {
      consumer.unsubscribeAsync()
    }.liftF.void

    override def getLastMessageId[T](consumer: api.Consumer[T]): F[MessageId] = Async[F].delay {
      consumer.getLastMessageIdAsync
    }.liftF.map(MessageId.fromJava)

    override def close(producer: api.Producer[_]): F[Unit] = Async[F].delay {
      producer.closeAsync()
    }.liftF.void

    override def close(consumer: api.Consumer[_]): F[Unit] = Async[F].delay {
      consumer.closeAsync()
    }.liftF.void

    override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): F[Unit] = Async[F].delay {
      consumer.seekAsync(messageId)
    }.liftF.void

    override def seekAsync(consumer: JConsumer[_], timestamp: Long): F[Unit] = Async[F].delay {
      consumer.seekAsync(timestamp)
    }.liftF.void

    override def seekAsync(reader: api.Reader[_], messageId: MessageId): F[Unit] = Async[F].delay {
      reader.seekAsync(messageId)
    }.liftF.void

    override def seekAsync(reader: api.Reader[_], timestamp: Long): F[Unit] = Async[F].delay {
      reader.seekAsync(timestamp)
    }.liftF.void

    override def transform[A, B](t: F[A])(fn: A => Try[B]): F[B] =
      t.flatMap { a =>
        fn(a) match {
          case Success(b) => Async[F].pure(b)
          case Failure(e) => Async[F].raiseError(e)
        }
      }

    override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      Async[F].delay(consumer.acknowledgeAsync(messageId)).liftF.void

    override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId, txn: Transaction): F[Unit] =
      Async[F].delay(consumer.acknowledgeAsync(messageId, txn)).liftF.void

    override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      Async[F].delay(consumer.acknowledgeCumulativeAsync(messageId)).liftF.void

    override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId,
                                               txn: Transaction): F[Unit] =
      Async[F].delay(consumer.acknowledgeCumulativeAsync(messageId, txn)).liftF.void

    override def negativeAcknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      Async[F].delay {
        consumer.negativeAcknowledge(messageId)
      }

    override def close(reader: api.Reader[_]): F[Unit] = Async[F].delay {
      reader.closeAsync()
    }.liftF.void

    override def flush(producer: api.Producer[_]): F[Unit] = Async[F].delay {
      producer.flushAsync()
    }.liftF.void

    override def close(client: api.PulsarClient): F[Unit] = Async[F].delay {
      client.closeAsync()
    }.liftF.void

    override def nextAsync[T](reader: api.Reader[T]): F[ConsumerMessage[T]] =
      Async[F].delay {
        reader.readNextAsync()
      }.liftF.map(ConsumerMessage.fromJava)


    override def hasMessageAvailable(reader: api.Reader[_]): F[Boolean] = Async[F].delay {
      reader.hasMessageAvailableAsync
    }.liftF.map(identity(_))

    override def send[T](builder: TypedMessageBuilder[T]): F[MessageId] =
      Async[F].delay(builder.sendAsync()).liftF.map(MessageId.fromJava)

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
      Async[F].delay(builder.build()).liftF.map(TransactionContext(_))

    def commitTransaction(txn: Transaction): F[Unit] = Async[F].delay(txn.commit()).liftF.map(_ => ())

    def abortTransaction(txn: Transaction): F[Unit] = Async[F].delay(txn.abort()).liftF.map(_ => ())

  }

}
