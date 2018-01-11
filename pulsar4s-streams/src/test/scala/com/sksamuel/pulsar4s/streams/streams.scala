package com.sksamuel.pulsar4s

package object streams {

  val DEFAULT_TIMEOUT_MILLIS = 2000l
  val PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 2000L

  val castles = Array(
    Message("bodium"),
    Message("hever"),
    Message("tower of london"),
    Message("canarvon"),
    Message("conwy"),
    Message("beaumaris"),
    Message("bolsover"),
    Message("conningsbrough"),
    Message("tintagel"),
    Message("rochester"),
    Message("dover"),
    Message("hexham"),
    Message("harleigh"),
    Message("white"),
    Message("radley"),
    Message("berkeley")
  )
}
