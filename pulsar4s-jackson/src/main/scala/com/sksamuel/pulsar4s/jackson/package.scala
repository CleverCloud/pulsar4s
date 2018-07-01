package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

package object jackson {
  implicit def schema[T: Manifest]: Schema[T] = new Schema[T] {
    override def encode(t: T): Array[Byte] = JacksonSupport.mapper.writeValueAsBytes(t)
    override def decode(bytes: Array[Byte]): T = JacksonSupport.mapper.readValue[T](bytes)
    override def getSchemaInfo: SchemaInfo = {
      val info = new SchemaInfo()
      info.setName(manifest[T].runtimeClass.getCanonicalName)
      info.setType(SchemaType.JSON)
      info
    }
  }
}
