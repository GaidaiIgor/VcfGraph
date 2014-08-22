import org.apache.spark.graphx._

// alleles is a list of alleles available from this vertex, _1 is a allele string itself,
// _2 is array of vertexIds belong to that allele
case class VertexAttr(nucleotide: Char, isReference: Boolean, pos: Long, var alleles: List[(String, Array[VertexId])])
{
  override def toString() =
  {
    if (isReference)
    {
      nucleotide.toString ++ " R"
    }
    else
    {
      nucleotide.toString
    }
  }
}
