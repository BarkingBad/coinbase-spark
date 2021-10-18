package pl.edu.agh.model

import io.circe.{Decoder, Encoder}
import io.circe.parser.parse
import io.circe.syntax._

object Parser {

  def apply[T](
      json: String
  )(implicit decoder: Decoder[T]): Either[io.circe.Error, T] = {
    parse(json).flatMap(_.hcursor.as[T])
  }
}

object Serializer {
  def apply[T](obj: T)(implicit encoder: Encoder[T]): String =
    obj.asJson.toString()
}
