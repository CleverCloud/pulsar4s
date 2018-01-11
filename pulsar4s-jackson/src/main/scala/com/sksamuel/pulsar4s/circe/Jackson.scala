package com.sksamuel.pulsar4s.circe

import com.sksamuel.pulsar4s.{Message, MessageReader, MessageWriter}

import scala.util.Try

object Jackson {

  implicit def writer[T: Manifest]: MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Try[Message] = {
      Try {
        val bytes = JacksonSupport.mapper.writeValueAsBytes(t)
        Message(None, bytes, Map.empty, None, 0, System.currentTimeMillis())
      }
    }
  }

  implicit def reader[T: Manifest]: MessageReader[T] = new MessageReader[T] {
    override def read(msg: Message): Try[T] = {
      Try {
        JacksonSupport.mapper.readValue[T](msg.data)
      }
    }
  }

}
