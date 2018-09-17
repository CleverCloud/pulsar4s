package com.sksamuel.pulsar4s.akka.streams

import akka.Done
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.{Producer, ProducerMessage}

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success}

class PulsarSinkGraphStage[T](createFn: () => Producer[T])
  extends GraphStageWithMaterializedValue[SinkShape[ProducerMessage[T]], Future[Done]]
    with Logging {

  private val in = Inlet.create[ProducerMessage[T]]("pulsar.in")
  override def shape: SinkShape[ProducerMessage[T]] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {

    val promise = Promise[Done]()

    val logic: GraphStageLogic = new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      implicit def context: ExecutionContextExecutor = super.materializer.executionContext

      var producer: Producer[T] = _
      var next: AsyncCallback[ProducerMessage[T]] = _
      var error: Throwable = _

      override def preStart(): Unit = {
        producer = createFn()
        next = getAsyncCallback { _ => pull(in) }
        pull(in)
      }

      override def onPush(): Unit = {
        try {
          val t = grab(in)
          logger.debug(s"Sending message $t")
          producer.sendAsync(t).onComplete {
            case Success(_) => next.invoke(t)
            case Failure(e) =>
              logger.error("Failing pulsar sink stage", e)
              failStage(e)
          }
        } catch {
          case e: Throwable =>
            logger.error("Failing pulsar sink stage", e)
            failStage(e)
        }
      }

      override def postStop(): Unit = {
        logger.debug("Graph stage stopping; closing producer")
        producer.flush()
        producer.close()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        promise.tryFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        promise.trySuccess(Done)
      }
    }

    (logic, promise.future)
  }
}
