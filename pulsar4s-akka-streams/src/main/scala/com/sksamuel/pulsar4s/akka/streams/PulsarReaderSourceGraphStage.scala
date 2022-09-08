package com.sksamuel.pulsar4s.akka.streams

import akka.Done
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.{ConsumerMessage, MessageId, Reader}
import org.apache.pulsar.client.api.ConsumerStats

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class PulsarReaderSourceGraphStage[T](create: () => Reader[T], seek: Option[MessageId]) extends GraphStageWithMaterializedValue[SourceShape[ConsumerMessage[T]], Control] with Logging {

  private val out = Outlet[ConsumerMessage[T]]("pulsar.out")
  override def shape: SourceShape[ConsumerMessage[T]] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {

    val logic: GraphStageLogic with Control = new GraphStageLogic(shape) with OutHandler with Control {
      setHandler(out, this)

      implicit def ec: ExecutionContext = materializer.executionContext

      @inline private def reader: Reader[T] = consumerOpt.getOrElse(throw new IllegalStateException("Reader not initialized!"))
      private var consumerOpt: Option[Reader[T]] = None
      private val receiveCallback: AsyncCallback[Try[ConsumerMessage[T]]] = getAsyncCallback {
        case Success(msg) =>
          push(out, msg)
        case Failure(e) =>
          failStage(e)
      }
      private val stopped: Promise[Done] = Promise()
      private val stopCallback: AsyncCallback[Unit] = getAsyncCallback { _ => completeStage() }

      override def preStart(): Unit = {
        try {
          val reader = create()
          consumerOpt = Some(reader)
          stopped.future.onComplete { _ =>close()}
          seek foreach reader.seek
        } catch {
          case NonFatal(e) =>
            logger.error("Error creating reader!", e)
            failStage(e)
        }
      }

      override def onPull(): Unit = {
        logger.debug("Pull received; asking reader for message")
        reader.nextAsync.onComplete(receiveCallback.invoke(_))
      }

      override def postStop(): Unit = stopped.success(Done)

      override def complete()(implicit ec: ExecutionContext): Future[Done] = {
        stopCallback.invoke(())
        stopped.future
      }

      private def close()(implicit ec: ExecutionContext): Future[Done] =
        consumerOpt.fold(Future.successful(Done))(_.closeAsync.map(_ => Done))

      override def shutdown()(implicit ec: ExecutionContext): Future[Done] =
        for {
          _ <- complete()
          _ <- close()
        } yield Done

      override def stats: ConsumerStats = ???
    }

    (logic, logic)
  }
}
