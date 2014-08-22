import java.io.{ObjectInputStream, InputStream, ObjectOutputStream, OutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId, Graph}

import scala.reflect._

object Serialization
{
  def serialize[VD, ED](graph: Graph[VD, ED], output_stream: OutputStream) =
  {
    val oos = new ObjectOutputStream(output_stream)
    oos.writeObject(graph.vertices.collect())
    oos.writeObject(graph.edges.collect())
  }

  def serialize(vertices: Array[(VertexId, VertexAttr)], edges: Array[Edge[EdgeAttr]], output_stream: OutputStream) =
  {
    val oos = new ObjectOutputStream(output_stream)
    oos.writeObject(vertices)
    oos.writeObject(edges)
  }

  def deserialize(input_stream: InputStream): (Array[(VertexId, VertexAttr)], Array[Edge[EdgeAttr]]) =
  {
    val ois = new ObjectInputStream(input_stream)
    val vertices = ois.readObject().asInstanceOf[Array[(VertexId, VertexAttr)]]
    val edges = ois.readObject().asInstanceOf[Array[Edge[EdgeAttr]]]
    (vertices, edges)
  }

  def deserialize[VD:ClassTag, ED:ClassTag](sc: SparkContext, input_stream: InputStream): Graph[VD, ED] =
  {
    val ois = new ObjectInputStream(input_stream)
    val vertices = ois.readObject().asInstanceOf[Array[(VertexId, VD)]]
    val edges = ois.readObject().asInstanceOf[Array[Edge[ED]]]

    val verticesRDD = sc.parallelize(vertices)
    val edgesRDD = sc.parallelize(edges)

    Graph(verticesRDD, edgesRDD)
  }
}
