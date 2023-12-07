package com.sksamuel.pulsar4s.pekko.streams

import org.apache.pekko.Done
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.SourceShape
import org.apache.pekko.stream.stage.AsyncCallback
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.GraphStageWithMaterializedValue
import org.apache.pekko.stream.stage.OutHandler
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.Consumer
import com.sksamuel.pulsar4s.ConsumerMessage
import com.sksamuel.pulsar4s.MessageId
import com.sksamuel.pulsar4s.TransactionContext
import org.apache.pulsar.client.api.ConsumerStats

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

trait CommittableMessage[T] extends TransactionalCommittableMessageOps {
  def nack(): Future[Done]
  def message: ConsumerMessage[T]
  def tx(implicit txn: TransactionContext): TransactionalCommittableMessageOps
}

trait TransactionalCommittableMessageOps {
  def ack(cumulative: Boolean = false): Future[Done]
}

class PulsarCommittableSourceGraphStage[T](
  create: () => Consumer[T],
  seek: Option[MessageId],
  closeDelay: FiniteDuration,
) extends GraphStageWithMaterializedValue[SourceShape[CommittableMessage[T]], Control]
  with Logging {

  @deprecated("Use main constructor", "2.7.1")
  def this(create: () => Consumer[T], seek: Option[MessageId]) = this(create, seek, closeDelay = DefaultCloseDelay)

  private val out = Outlet[CommittableMessage[T]]("pulsar.out")
  override def shape: SourceShape[CommittableMessage[T]] = SourceShape(out)

  private class CommittableMessageImpl[T](
    val consumer: Consumer[T],
    val message: ConsumerMessage[T],
    val ctx: Option[TransactionContext] = None
  )(implicit ec: ExecutionContext) extends CommittableMessage[T] {
    def messageId: MessageId = message.messageId
    override def ack(cumulative: Boolean): Future[Done] = {
      logger.debug(s"Acknowledging message: $message")
      val txnOps = ctx.map(consumer.tx(_)).getOrElse(consumer)
      val ackFuture = if (cumulative) {
        txnOps.acknowledgeCumulativeAsync(message.messageId)
      } else {
        txnOps.acknowledgeAsync(message.messageId)
      }
      ackFuture.map(_ => Done)
    }
    override def tx(implicit ctx: TransactionContext): TransactionalCommittableMessageOps = {
      new CommittableMessageImpl(consumer, message, Some(ctx))
    }
    override def nack(): Future[Done] = {
      logger.debug(s"Negatively acknowledging message: $message")
      consumer.negativeAcknowledgeAsync(message.messageId).map(_ => Done)
    }
  }

  private class PulsarCommittableSourceLogic(shape: Shape) extends GraphStageLogic(shape) with OutHandler with Control {
    setHandler(out, this)

    implicit def ec: ExecutionContext = materializer.executionContext

    @inline private def consumer: Consumer[T] =
      consumerOpt.getOrElse(throw new IllegalStateException("Consumer not initialized!"))
    private var consumerOpt: Option[Consumer[T]] = None
    private var receiveCallback: AsyncCallback[Try[ConsumerMessage[T]]] = getAsyncCallback {
      case Success(msg) =>
        logger.debug(s"Message received: $msg")
        push(out, new CommittableMessageImpl(consumer, msg))
      case Failure(e) =>
        logger.warn("Error when receiving message", e)
        failStage(e)
    }
    private val stopped: Promise[Done] = Promise()
    private val stopCallback: AsyncCallback[Unit] = getAsyncCallback(_ => completeStage())

    override def preStart(): Unit = {
      try {
        val consumer = create()
        consumerOpt = Some(consumer)
        stopped.future.onComplete { _ =>
          // Schedule to stop after a delay to give unacked messages time to finish
          materializer.scheduleOnce(closeDelay, () => close())
        }
        seek foreach consumer.seek
      } catch {
        case NonFatal(e) =>
          logger.error("Error creating consumer!", e)
          failStage(e)
      }
    }

    override def onPull(): Unit = {
      logger.debug("Pull received; asking consumer for message")
      consumer.receiveAsync.onComplete(receiveCallback.invoke)
    }

    private def close()(implicit ec: ExecutionContext): Future[Done] = {
      consumerOpt.fold(Future.successful(Done))(_.closeAsync.map(_ => Done))
    }

    override def complete()(implicit ec: ExecutionContext): Future[Done] = {
      stopCallback.invoke(())
      stopped.future
    }

    override def postStop(): Unit = stopped.success(Done)
    
    override def shutdown()(implicit ec: ExecutionContext): Future[Done] = {
      for {
        _ <- complete()
        _ <- close()
      } yield Done
    }

    override def stats: ConsumerStats = consumer.stats
  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new PulsarCommittableSourceLogic(shape)
    (logic, logic)
  }
}

