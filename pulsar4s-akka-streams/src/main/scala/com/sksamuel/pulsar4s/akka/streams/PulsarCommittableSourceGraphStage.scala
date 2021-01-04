package com.sksamuel.pulsar4s.akka.streams

import akka.Done
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.OutHandler
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.Consumer
import com.sksamuel.pulsar4s.ConsumerMessage
import com.sksamuel.pulsar4s.MessageId
import org.apache.pulsar.client.api.ConsumerStats

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

trait CommittableMessage[T] {
  def ack(cumulative: Boolean = false): Future[Done]
  def nack(): Future[Done]
  def message: ConsumerMessage[T]
}

class PulsarCommittableSourceGraphStage[T](create: () => Consumer[T], seek: Option[MessageId])
  extends GraphStageWithMaterializedValue[SourceShape[CommittableMessage[T]], Control]
    with Logging {

  private val out = Outlet[CommittableMessage[T]]("pulsar.out")
  override def shape: SourceShape[CommittableMessage[T]] = SourceShape(out)

  private class CommittableMessageImpl[T](
    val consumer: Consumer[T],
    val message: ConsumerMessage[T]
  )(implicit ec: ExecutionContext) extends CommittableMessage[T] {
    def messageId: MessageId = message.messageId
    override def ack(cumulative: Boolean): Future[Done] = {
      logger.debug(s"Acknowledging message: $message")
      val ackFuture = if (cumulative) {
        consumer.acknowledgeCumulativeAsync(message.messageId)
      } else {
        consumer.acknowledgeAsync(message.messageId)
      }
      ackFuture.map(_ => Done)
    }
    override def nack(): Future[Done] = {
      logger.debug(s"Negatively acknowledging message: $message")
      consumer.negativeAcknowledgeAsync(message.messageId).map(_ => Done)
    }
  }

  private class PulsarCommittableSourceLogic(shape: Shape) extends GraphStageLogic(shape) with OutHandler with Control {
    setHandler(out, this)

    @inline private def consumer: Consumer[T] =
      consumerOpt.getOrElse(throw new IllegalStateException("Consumer not initialized!"))
    private var consumerOpt: Option[Consumer[T]] = None
    private var receiveCallback: AsyncCallback[Try[ConsumerMessage[T]]] = _
    private val stopped: Promise[Done] = Promise()
    private val stopCallback: AsyncCallback[Unit] = getAsyncCallback(_ => completeStage())

    override def preStart(): Unit = {
      try {
        implicit val context: ExecutionContext = super.materializer.executionContext
        consumerOpt = Some(create())
        seek foreach consumer.seek
        receiveCallback = getAsyncCallback {
          case Success(msg) =>
            logger.debug(s"Message received: $msg")
            push(out, new CommittableMessageImpl(consumer, msg))
          case Failure(e) =>
            logger.warn("Error when receiving message", e)
            failStage(e)
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Error creating consumer!", e)
          failStage(e)
      }
    }

    override def onPull(): Unit = {
      implicit val context: ExecutionContext = super.materializer.executionContext
      logger.debug("Pull received; asking consumer for message")
      consumer.receiveAsync.onComplete(receiveCallback.invoke)
    }

    override def complete()(implicit ec: ExecutionContext): Future[Done] = {
      stopCallback.invoke(())
      stopped.future
    }

    override def postStop(): Unit = stopped.success(Done)

    override def shutdown()(implicit ec: ExecutionContext): Future[Done] =
      for {
        _ <- complete()
        _ <- consumerOpt.fold(Future.successful(()))(_.closeAsync)
      } yield Done

    def stats: ConsumerStats = consumer.stats
  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new PulsarCommittableSourceLogic(shape)
    (logic, logic)
  }
}

