package com.sksamuel.pulsar4s.jackson

import com.sksamuel.pulsar4s.{MessageReader, MessageWriter, SMessage}
import org.apache.pulsar.client.api.Message

object Jackson {

  implicit def writer[T: Manifest]: MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Message = ???
  }

  implicit def reader[T: Manifest]: MessageReader[T] = new MessageReader[T] {
    override def read(msg: SMessage): T = JacksonSupport.mapper.readValue[T](msg.data)
  }

}
