package com.sksamuel

package object pulsar4s {
  type JMessageId = org.apache.pulsar.client.api.MessageId
  type JMessage[T] = org.apache.pulsar.client.api.Message[T]
  type JConsumer[T] = org.apache.pulsar.client.api.Consumer[T]
  type JProducer[T] = org.apache.pulsar.client.api.Producer[T]
}
