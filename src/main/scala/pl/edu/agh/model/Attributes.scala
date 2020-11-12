package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
sealed trait Attributes

@JsonCodec
case class Name(name: String) extends Attributes

@JsonCodec
case class Url(url: String) extends Attributes

@JsonCodec
case class SourceName(sourceName: String) extends Attributes

@JsonCodec
case class SourceType(sourceType: String) extends Attributes

@JsonCodec
case class Mobile(mobile: Boolean) extends Attributes
