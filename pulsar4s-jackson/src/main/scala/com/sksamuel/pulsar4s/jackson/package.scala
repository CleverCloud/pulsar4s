package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

package object jackson {
  implicit def schema[T: Manifest]: Schema[T] = new Schema[T] {
    override def encode(t: T): Array[Byte] = JacksonSupport.mapper.writeValueAsBytes(t)
    override def decode(bytes: Array[Byte]): T = JacksonSupport.mapper.readValue[T](bytes)
    override def getSchemaInfo: SchemaInfo =
      new SchemaInfo()
        .setName(manifest[T].runtimeClass.getCanonicalName)
        .setType(SchemaType.JSON)
        .setSchema("""{"type":"any"}""".getBytes("UTF-8"))
  }
}
