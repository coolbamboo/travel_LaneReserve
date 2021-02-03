package utils

object VertexEnum extends Enumeration {
  type VertexEnum = Value

  val start: utils.VertexEnum.Value = Value(0)
  val end: utils.VertexEnum.Value = Value(1)
  val pass: utils.VertexEnum.Value = Value(2)
  val LR: utils.VertexEnum.Value = Value(3)
  val unknown: utils.VertexEnum.Value = Value(-1)
}
