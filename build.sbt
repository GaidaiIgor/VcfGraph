name := "VCF_Graph"

version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.0.2",
  "org.apache.spark" % "spark-graphx_2.10" % "1.0.2",
  "org.utgenome.thirdparty" % "picard" % "1.102.0"
)