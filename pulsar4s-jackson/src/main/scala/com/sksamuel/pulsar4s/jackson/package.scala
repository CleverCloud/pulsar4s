package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema

package object jackson {
  implicit def schema[T: Manifest]: Schema[T] = new Schema[T] {
    override def encode(t: T): Array[Byte] = JacksonSupport.mapper.writeValueAsBytes(t)
    override def decode(bytes: Array[Byte]): T = JacksonSupport.mapper.readValue[T](bytes)
    override def getSchemaInfo: org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo = {
      val info = new org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo()
      info.setName(manifest[T].runtimeClass.getCanonicalName)
      info.setType(org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType.JSON)
      info
    }
  }
}
