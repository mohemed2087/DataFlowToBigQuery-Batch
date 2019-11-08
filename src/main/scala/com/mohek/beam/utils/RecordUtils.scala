package com.mohek.beam.utils

import scala.util.{Failure, Success}

object RecordUtils {

  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

  def toFloat(s: String): Option[Float] = {
    try {
      Some(s.trim.toFloat)
    } catch {
      case e: Exception => None
    }
  }

}
