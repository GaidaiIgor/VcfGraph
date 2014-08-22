import java.io.{FileInputStream, FileOutputStream}

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.io.Source

object Test
{
  val configuration = new SparkConf().setAppName("VcfGraph").setMaster("local")
  val context = new SparkContext(configuration)

  var test_graph = VcfGraph.fromReference(Source.fromFile("reference").mkString, context)

  def print_graph(graph: Graph[VertexAttr, EdgeAttr]) =
  {
    graph.triplets.map(
      triplet => triplet.srcAttr.toString() + "->" + triplet.attr.toString() + "->" + triplet.dstAttr.toString()
    ).collect.foreach(println(_))
  }

  def serialization_test() =
  {
    val output = new FileOutputStream("test")
    Serialization.serialize(test_graph, output)
    output.close()

    val input = new FileInputStream("test")
    val graph = Serialization.deserialize[(String, String), String](context, input)
    input.close()

    print_graph(test_graph)
  }

  def fromReference_test() =
  {
    val reference = Source.fromFile("reference").mkString
    print_graph(VcfGraph.fromReference(reference, context))
  }

  def toDOT_test() =
  {
    VcfGraph.toDot(test_graph, new FileOutputStream("test.dot"))
  }

  def create_test_reference_graphs(): Unit =
  {
    val graph = VcfGraph.fromReference("atcga", context)
    Serialization.serialize(graph, new FileOutputStream("reference_1"))
    val graph_2 = VcfGraph.fromReference("atcga", context)
    Serialization.serialize(graph_2, new FileOutputStream("reference_2"))
  }

  def addVcf_test() =
  {
    create_test_reference_graphs()

    VcfGraph.addVcf(new FileInputStream("test2.vcf"), context)
//    VcfGraph.addVcf(new FileInputStream("test2.vcf"), context)

    test_graph = Serialization.deserialize[VertexAttr, EdgeAttr](context, new FileInputStream("reference_1"))
    VcfGraph.toDot(test_graph, new FileOutputStream("test_1.dot"))

//    test_graph = Serialization.deserialize[VertexAttr, EdgeAttr](context, new FileInputStream("reference_2"))
//    VcfGraph.toDot(test_graph, new FileOutputStream("test_2.dot"))
  }

  def toVcf_test() =
  {
    create_test_reference_graphs()

    VcfGraph.addVcf(new FileInputStream("test2.vcf"), context)

    test_graph = Serialization.deserialize[VertexAttr, EdgeAttr](context, new FileInputStream("reference_1"))
    VcfGraph.toDot(test_graph, new FileOutputStream("test_1.dot"))

    VcfGraph.toVcf("reference", List("1"), new FileOutputStream("out.vcf"))

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    create_test_reference_graphs()

    VcfGraph.addVcf(new FileInputStream("out.vcf"), context)

    test_graph = Serialization.deserialize[VertexAttr, EdgeAttr](context, new FileInputStream("reference_1"))
    VcfGraph.toDot(test_graph, new FileOutputStream("out.dot"))
  }
}
