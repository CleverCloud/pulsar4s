package com.sksamuel.pulsar4s.pekko.streams

import org.apache.pekko.Done
import org.apache.pekko.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import org.apache.pekko.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.{Producer, ProducerMessage}

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

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
      var produceCallback: AsyncCallback[Try[_]] = _
      var error: Throwable = _

      override def preStart(): Unit = {
        producer = createFn()
        produceCallback = getAsyncCallback {
          case Success(_) =>
            pull(in)
          case Failure(e) =>
            logger.error("Failing pulsar sink stage", e)
            failStage(e)
        }
        pull(in)
      }

      override def onPush(): Unit = {
        try {
          val t = grab(in)
          logger.debug(s"Sending message $t")
          producer.sendAsync(t).onComplete(produceCallback.invoke)
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
