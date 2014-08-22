import java.io.{ObjectInputStream, FileInputStream, ObjectOutputStream, FileOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object Main
{
  def main(args: Array[String]) =
  {
//    Test.fromReference_test()
//    Test.serialization_test()
//    Test.toDOT_test()
//    Test.addVcf_test()
    Test.toVcf_test()
  }
}
