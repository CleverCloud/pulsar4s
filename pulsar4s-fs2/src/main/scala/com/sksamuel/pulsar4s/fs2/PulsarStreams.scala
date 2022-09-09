package com.sksamuel.pulsar4s.fs2

import cats.effect.{MonadCancel, MonadCancelThrow, Resource}
import com.sksamuel.pulsar4s._

trait CommittableMessage[F[_], X] {
  def ack: F[Unit]
  def nack: F[Unit]
  def data: X

  def map[Y](f: X => Y): CommittableMessage[F, Y]
}

object PulsarStreams {
  import _root_.fs2.{Pipe, Stream}

  private final class DelegateCommittableMessage[F[_] : AsyncHandler, T](
    message: ConsumerCommittableMessage[F, _],
    payload: T
  ) extends CommittableMessage[F, T] {
    override def ack: F[Unit] = message.ack
    override def nack: F[Unit] = message.nack
    override def data: T = payload
    override def map[Y](f: T => Y): CommittableMessage[F, Y] = new DelegateCommittableMessage(message, f(payload))
  }

  private final case class ConsumerCommittableMessage[F[_] : AsyncHandler, T](
    message: ConsumerMessage[T],
    consumer: Consumer[T]
  ) extends CommittableMessage[F, ConsumerMessage[T]] {
    override def ack: F[Unit] = consumer.acknowledgeAsync(message.messageId)
    override def nack: F[Unit] = consumer.negativeAcknowledgeAsync(message.messageId)
    override def data: ConsumerMessage[T] = message

    override def map[Y](f: ConsumerMessage[T] => Y): CommittableMessage[F, Y] =
      new DelegateCommittableMessage(this, f(message))
  }

  def batch[F[_] : MonadCancelThrow : AsyncHandler, T](
    consumer: F[Consumer[T]]
  ): Stream[F, CommittableMessage[F, ConsumerMessage[T]]] =
    Stream.resource(Resource.make(consumer)(_.closeAsync))
      .flatMap { consumer =>
        Stream
          .repeatEval(consumer.receiveBatchAsync[F])
          .flatMap(Stream.emits(_))
          .mapChunks(_.map(message => ConsumerCommittableMessage(message, consumer)))
      }

  def single[F[_] : MonadCancelThrow : AsyncHandler, T](
    consumer: F[Consumer[T]]
  ): Stream[F, CommittableMessage[F, ConsumerMessage[T]]] =
    Stream.resource(Resource.make(consumer)(_.closeAsync))
      .flatMap { consumer =>
        Stream
          .repeatEval(consumer.receiveAsync[F])
          .mapChunks(_.map(message => ConsumerCommittableMessage(message, consumer)))
      }

  def reader[F[_] : MonadCancelThrow : AsyncHandler, T](
    reader: F[Reader[T]]
  ): Stream[F, ConsumerMessage[T]] =
    Stream.resource(Resource.make(reader)(_.closeAsync))
      .flatMap { reader =>
        Stream
          .repeatEval(reader.nextAsync[F])
      }

  def sink[F[_] : MonadCancelThrow : AsyncHandler, T](
    producer: F[Producer[T]]
  ): Pipe[F, ProducerMessage[T], MessageId] = messages =>
    Stream.resource(Resource.make(producer)(_.closeAsync))
      .flatMap { producer =>
        messages.evalMap(producer.sendAsync(_))
      }

  def committableSink[F[_] : MonadCancelThrow : AsyncHandler , T](
    producer: F[Producer[T]]
  ): Pipe[F, CommittableMessage[F, ProducerMessage[T]], MessageId] = messages =>
    Stream.resource(Resource.make(producer)(_.closeAsync))
      .flatMap { producer =>
        messages.evalMap { message =>
          MonadCancel[F, Throwable].guaranteeCase(producer.sendAsync(message.data)) { out =>
            if(out.isSuccess) message.ack else message.nack
          }
        }
      }
}