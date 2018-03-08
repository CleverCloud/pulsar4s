package com.sksamuel.pulsar4s

package object streams {

  val DEFAULT_TIMEOUT_MILLIS = 2000l
  val PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 2000L

  val castles = Array(
    Message("beaumaris"),
    Message("berkeley"),
    Message("bodium"),
    Message("bolsover"),
    Message("caernarfon"),
    Message("carisbrooke"),
    Message("conningsbrough"),
    Message("conwy"),
    Message("dover"),
    Message("harleigh"),
    Message("hexham"),
    Message("hever"),
    Message("leeds"),
    Message("raby"),
    Message("radley"),
    Message("rochester"),
    Message("tintagel"),
    Message("tower of london"),
    Message("warwick"),
    Message("white")
  )
}
