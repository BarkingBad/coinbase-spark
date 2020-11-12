package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class PeriodStruct(value: Option[Double], unit: Option[String])
