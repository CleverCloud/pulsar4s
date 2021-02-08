package com.sksamuel.pulsar4s.cats

import java.util.concurrent.{CompletableFuture, CompletionException}

import cats.effect._
import cats.implicits._
import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{ConsumerBuilder, ProducerBuilder, PulsarClient, Reader, ReaderBuilder, TypedMessageBuilder}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.ExecutionException
import scala.language.higherKinds
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


object CatsAsyncHandler extends CatsAsyncHandlerLowPriority {
  implicit def handler: AsyncHandler[IO] = asyncHandlerForCatsEffectAsync[IO]
}

trait CatsAsyncHandlerLowPriority {

  object CompletableFutureConverters {

    implicit class CompletableOps[T](f: => CompletableFuture[T]) {
      def toF[F[_] : Async]: F[T] =
        Async[F].suspend {
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
            Async[F].async[T] { k =>
              f.whenCompleteAsync((t: T, e: Throwable) => {
                if (e != null) k.apply(Left(e))
                else k.apply(Right(t))
              })
            }
          }
        }
    }

  }

  implicit def asyncHandlerForCatsEffectAsync[F[_] : Async]: AsyncHandler[F] = new AsyncHandler[F] {

    import CompletableFutureConverters._

    override def failed(e: Throwable): F[Nothing] = Async[F].raiseError(e)

    override def createProducer[T](builder: ProducerBuilder[T]): F[Producer[T]] = builder.createAsync().toF[F].map(new DefaultProducer(_))

    override def createConsumer[T](builder: ConsumerBuilder[T]): F[Consumer[T]] = builder.subscribeAsync().toF[F].map(new DefaultConsumer(_))

    override def createReader[T](builder: ReaderBuilder[T]): F[pulsar4s.Reader[T]] = builder.createAsync().toF[F].map(new DefaultReader(_))

    override def send[T](t: T, producer: api.Producer[T]): F[MessageId] = producer.sendAsync(t).toF[F].map(MessageId.fromJava)

    override def receive[T](consumer: api.Consumer[T]): F[ConsumerMessage[T]] = consumer.receiveAsync().toF[F].map(ConsumerMessage.fromJava)

    override def receiveBatch[T](consumer: api.Consumer[T]): F[Vector[ConsumerMessage[T]]] =
      consumer.batchReceiveAsync().toF[F].map(_.asScala.map(ConsumerMessage.fromJava).toVector)

    override def unsubscribeAsync(consumer: api.Consumer[_]): F[Unit] = consumer.unsubscribeAsync().toF[F].void

    override def getLastMessageId[T](consumer: api.Consumer[T]): F[MessageId] = consumer.getLastMessageIdAsync.toF[F].map(MessageId.fromJava)

    override def close(producer: api.Producer[_]): F[Unit] = producer.closeAsync().toF[F].void

    override def close(consumer: api.Consumer[_]): F[Unit] = consumer.closeAsync().toF[F].void

    override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): F[Unit] = consumer.seekAsync(messageId).toF[F].void

    override def seekAsync(reader: api.Reader[_], messageId: MessageId): F[Unit] = reader.seekAsync(messageId).toF[F].void

    override def seekAsync(reader: api.Reader[_], timestamp: Long): F[Unit] = reader.seekAsync(timestamp).toF[F].void

    override def transform[A, B](t: F[A])(fn: A => Try[B]): F[B] =
      t.flatMap { a =>
        fn(a) match {
          case Success(b) => Async[F].pure(b)
          case Failure(e) => Async[F].raiseError(e)
        }
      }

    override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      consumer.acknowledgeAsync(messageId).toF[F].void

    override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      consumer.acknowledgeCumulativeAsync(messageId).toF[F].void

    override def negativeAcknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      Async[F].delay {
        consumer.negativeAcknowledge(messageId)
      }

    override def close(reader: Reader[_]): F[Unit] = reader.closeAsync().toF[F].void

    override def flush(producer: api.Producer[_]): F[Unit] = producer.flushAsync().toF[F].void

    override def close(client: PulsarClient): F[Unit] = client.closeAsync().toF[F].void

    override def nextAsync[T](reader: Reader[T]): F[ConsumerMessage[T]] =
      reader.readNextAsync().toF[F].map(ConsumerMessage.fromJava)

    override def send[T](builder: TypedMessageBuilder[T]): F[MessageId] =
      builder.sendAsync().toF[F].map(MessageId.fromJava)
  }

}
