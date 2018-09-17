package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Authentication

import scala.concurrent.duration.FiniteDuration

case class PulsarClientConfig(serviceUrl: String,
                              allowTlsInsecureConnection: Option[Boolean],
                              authentication: Option[Authentication],
                              connectionsPerBroker: Option[Int],
                              enableTcpNoDelay: Option[Boolean],
                              enableTls: Option[Boolean],
                              enableTlsHostnameVerification: Option[Boolean],
                              listenerThreads: Option[Int],
                              maxConcurrentLookupRequests: Option[Int],
                              maxNumberOfRejectedRequestPerConnection: Option[Int],
                              operationTimeout: Option[FiniteDuration],
                              keepAliveInterval: Option[FiniteDuration],
                              statsInterval: Option[FiniteDuration],
                              maxLookupRequests: Option[Int],
                              tlsTrustCertsFilePath: Option[String],
                              ioThreads: Option[Int])