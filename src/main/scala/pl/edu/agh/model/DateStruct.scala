package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class DateStruct(utc: Option[String], local: Option[String])
