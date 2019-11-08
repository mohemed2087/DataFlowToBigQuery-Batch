package com.mohek.beam.utils

object Constants extends Enumeration {
  val RECORD_TYPE_INDEX=0
  val HEADER_REGEX="DataHead"
  val RIGHT_RECORD_TYPE=2
  val LEFT_RECORD_TYPE=3
  val RIGHTREC_KEY_INDEX=3
  val LEFTREC_KEY_INDEX=2
  val LEFTREC_COLUMNS="Indicator,VersionNumber,UID,Code,Data"
  val RIGHTREC_COLUMNS="VersionNumber,CNumber,ID,UID,Weight"
}
