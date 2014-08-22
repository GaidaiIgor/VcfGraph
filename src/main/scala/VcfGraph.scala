import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexId, Edge, Graph}
import org.broadinstitute.variant.vcf.{VCFHeader, VCFCodec}
import scala.collection.JavaConversions._

object VcfGraph
{
  def fromReference(ref: String, sc: SparkContext): Graph[VertexAttr, EdgeAttr] =
  {
    // dummy start and end vertices
    val reference: String = "s" ++ ref ++ "e"
    val vertices = (0L until reference.length).zip(reference).map{case (id, nucl) => (id,
      new VertexAttr(nucl, true, id.toInt, List.empty))}
    val edges = (0L until reference.length - 1).map(id => new Edge(id, id + 1, new EdgeAttr()))
    Graph(sc.parallelize(vertices), sc.parallelize(edges))
  }

  def toDot(graph: Graph[VertexAttr, EdgeAttr], output: OutputStream) =
  {
    output.write("digraph vcf {\n".getBytes)
    graph.vertices.collect().foreach(vertex =>
      output.write((vertex._1.toString ++ " [label=\"" ++ vertex._1.toString ++ " " ++ vertex._2.toString() ++ "\"];\n")
        .getBytes))
    graph.triplets.collect().foreach(triplet =>
      output.write((triplet.srcId.toString ++ " -> " ++ triplet.dstId.toString ++ ";\n").getBytes))
    output.write("}".getBytes)
  }

  def toVcf(reference: String, chromosomes: List[String], output: OutputStream): Unit =
  {
    var allele_id: Int = 0
    output.write(getHeader(reference).getBytes)
    for (chr: String <- chromosomes)
    {
      val (vertices_array, _) = Serialization.deserialize(getGraphInputStream(reference, chr))
      val ref_vertices = vertices_array.takeWhile(_._2.isReference)
      val ref_str: String = vertices_array.map(_._2.nucleotide).mkString
      for (vertex <- ref_vertices; allele <- vertex._2.alleles)
      {
//        var vcf_ref: String = null
//        var vcf_alt: String = null
//
//        if (allele._1.isEmpty)
//        {
//
//        }

        output.write((chr ++ "\t" ++ vertex._1.toString ++ "\t" ++ allele_id.toString ++ "\t" ++
          ref_str.substring(vertex._1.toInt, allele._2.last.toInt) ++ "\t" ++ vertex._2.nucleotide.toString ++
          allele._1 ++ "\t1\tPASS\t.\n").getBytes)
        allele_id += 1
      }
    }
  }

  // IMPORTANT: reference vertices must have the same id and position
  def addVcf(vcf: InputStream, sc: SparkContext): Unit =
  {
    val codec = new VCFCodec
    val it = codec.makeSourceFromStream(new BufferedInputStream(vcf))
    val header = codec.readActualHeader(it).asInstanceOf[VCFHeader]
    val ref_name = header.getOtherHeaderLine("reference").getValue

    var chromosome: String = ""
    var next_chromosome: String = null
    while (it.hasNext)
    {
      val variant_context = codec.decode(it)
      next_chromosome = variant_context.getChr

      if (next_chromosome != chromosome)
      {
        updateGraphArrays(ref_name, chromosome, next_chromosome, sc)
        chromosome = next_chromosome
      }

      val alleles: List[String] = variant_context.getAlternateAlleles.toList.map(_.toString.toLowerCase.stripSuffix
        ("*"))
      val ref_allele: String = variant_context.getReference.toString.toLowerCase.stripSuffix("*")
      // in vcf pos starts from 1, so are we
      val pos = variant_context.getStart

      for (allele: String <- alleles)
      {
        addPath(pos, allele, ref_allele)
      }
    }

    saveGraphArrays(ref_name, next_chromosome)
  }

  // will store vertices collected from RDD
  private var vertices: Array[(VertexId, VertexAttr)] = null
  // will store edges collected from RDD
  private var edges: Array[Edge[EdgeAttr]] = null

//  1 param - allele that should be added to graph
//  2 param - list of alleles available from pos where allele should be added (it's a part of VertexAttr)
//  3 param - id of reference vertex where allele should end
//  array consists of vertices id belongs to concrete allele (including reference ids where allele starts and ends)
//  returns: _1 is a part of allele that should be added between _2 and _3. _4 are ids before new branch in allele,
// _5 - after
  private def findMostSuitableAllele(allele: String, variants: List[(String, Array[VertexId])],
    src_id: VertexId, dst_id: VertexId):
    (String, VertexId, VertexId, Array[VertexId], Array[VertexId]) =
  {
    var min_allele: String = allele
    var start_id: VertexId = src_id
    var end_id: VertexId = dst_id
    var best_path: (Array[VertexId], Array[VertexId]) = (Array(src_id), Array(dst_id))
    for (variant: (String, Array[VertexId]) <- variants)
    {
      // existing allele and new allele ends at the same place so we can strip suffix too
      if (variant._2.last == dst_id)
      {
        val (stripped_allele, stripped_variant, lcp, lcs) = stripCommonPrefixSuffix(allele, variant._1)

        if (stripped_allele.length < min_allele.length)
        {
          min_allele = stripped_allele
          start_id = variant._2(lcp)
          end_id = variant._2(lcp + stripped_variant.length + 1)
          best_path = (variant._2.take(lcp + 1), variant._2.takeRight(lcs + 1))
        }
      }
      else
      {
        val (stripped_allele, _, lcp) = stripCommonPrefix(allele, variant._1)

        if (stripped_allele.length < min_allele.length)
        {
          min_allele = stripped_allele
          start_id = variant._2(lcp)
          end_id = dst_id
          best_path = (variant._2.take(lcp + 1), Array.empty)
        }
      }
    }

    (min_allele, start_id, end_id, best_path._1, best_path._2)
  }

  // each letter in str1 may be common only with one letter in str2
  // for example, "a" and "aba" will return ("","ba") because prefix stripes first and 'a' letter already had
  // corresponding letter in prefix of str2 when suffix was handling
  // returns stripped str1 and str2 and length of prefix and suffix that was removed
  def stripCommonPrefixSuffix(str1: String, str2: String): (String, String, Int, Int) =
  {
    val withoutPrefix = stripCommonPrefix(str1, str2)
    val withoutSuffix = stripCommonPrefix(withoutPrefix._1.reverse, withoutPrefix._2.reverse)
    (withoutSuffix._1.reverse, withoutSuffix._2.reverse, withoutPrefix._3, withoutSuffix._3)
  }

  def stripCommonPrefix(str1: String, str2: String): (String, String, Int) =
  {
    val lcpLength: Int = str1.zip(0 until str1.length).takeWhile(pair => pair._2 < str2.length && pair._1 == str2
      (pair._2)).length
    (str1.substring(lcpLength), str2.substring(lcpLength), lcpLength)
  }

  // adds new allele to graph
  // 1 param - pos where mutation begins
  // 2 param - allele that should be added
  // 3 param - ref allele that specified allele should replace
  private def addPath(initail_pos: Int, full_allele: String, full_ref: String): Unit =
  {
    val (allele, ref, lcp, _) = stripCommonPrefixSuffix(full_allele, full_ref)
    val pos = initail_pos + lcp
    val (min_allele, start_id, end_id, first_path_ids, last_path_ids):
    (String, VertexId, VertexId, Array[VertexId], Array[VertexId]) =
      findMostSuitableAllele(allele, vertices(pos - 1)._2.alleles, pos - 1, pos + ref.length)

    var new_ids: Array[VertexId] = Array.empty
    if (!min_allele.isEmpty)
    {
      // add edge from allele to first new vertex
      edges :+= new Edge(start_id, vertices.length, new EdgeAttr)
      val id_pos_diff = vertices.length - vertices(start_id.toInt)._2.pos + 1
      new_ids = (vertices.length.toLong until vertices.length + min_allele.length).toArray
      // add new vertices
      vertices ++= new_ids.zip(min_allele).map{case (id, nucl) => (id.toLong, new VertexAttr(nucl, false,
        id - id_pos_diff, List.empty))}
      // add new edges between new vertices
      edges ++= new_ids.tail.map(id => new Edge(id - 1, id, new EdgeAttr))
      // add edge from last vertex to allele
      edges :+= new Edge(vertices.length - 1, end_id, new EdgeAttr)
    }
    else
    {
      val new_edge = new Edge(start_id, end_id, new EdgeAttr)
      if (!edges.contains(new_edge))
      {
        edges :+= new_edge
      }
    }

    // add info about new allele
    vertices(pos - 1)._2.alleles ::= ((allele, first_path_ids ++ new_ids ++ last_path_ids))
  }

  private def loadGraphArrays(reference: String, chromosome: String, sc: SparkContext): Unit =
  {
    val result: (Array[(VertexId, VertexAttr)], Array[Edge[EdgeAttr]]) = Serialization.deserialize(getGraphInputStream
      (reference, chromosome))
    vertices = result._1.sortWith(_._1 < _._1)
    edges = result._2
  }

  private def saveGraphArrays(reference: String, chromosome: String): Unit =
  {
    Serialization.serialize(vertices, edges, getGraphOutputStream(reference, chromosome))
  }

  private def updateGraphArrays(reference: String, current_chromosome: String, new_chromosome: String,
                                sc: SparkContext): Unit =
  {
    if (current_chromosome != "")
    {
      saveGraphArrays(reference, current_chromosome)
    }
    loadGraphArrays(reference, new_chromosome, sc)
  }

  private def getGraphInputStream(reference: String, chromosome: String): InputStream =
  {
    new FileInputStream(reference ++ "_" ++ chromosome)
  }
  
  private def getGraphOutputStream(reference: String, chromosome: String): OutputStream = 
  {
    new FileOutputStream(reference ++ "_" ++ chromosome)
  }

  private def getHeader(reference: String): String =
  {
    "##fileformat=VCFv4.0\n" +
    "##reference=reference\n" +
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
  }
}
