package link.rdcn.dacp.recipe

import link.rdcn.dacp.optree.fifo.DockerContainer
import link.rdcn.struct.DataFrame

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/12 21:07
 * @Modified By:
 */
trait FlowNode

trait Transformer11 extends FlowNode with Serializable {
  def transform(dataFrame: DataFrame): DataFrame
}

trait Transformer21 extends FlowNode with Serializable {
  def transform(leftDataFrame: DataFrame, rightDataFrame: DataFrame): DataFrame
}

case class RepositoryNode(
                           functionId: String,
                           args: Map[String, String] = Map.empty
                         ) extends FlowNode

case class FifoFileBundleFlowNode(
                                   command: Seq[String],
                                   inputFilePath: Seq[String],
                                   outputFilePath: Seq[String],
                                   dockerContainer: DockerContainer,
                                   inputFileType: Int = FileType.FIFO_BUFFER,
                                   outputFileType: Int = FileType.FIFO_BUFFER
                                 ) extends FlowNode

object FileType {
  val FIFO_BUFFER = 1
  // 命名管道或磁盘FIFO
  val RAM_FILE = 2
  // 纯内存文件
  val MMAP_FILE = 3 // 内存映射文件
}

case class FifoFileFlowNode(filePath: String) extends FlowNode

//只为DAG执行提供dataFrameName
case class SourceNode(dataFrameName: String) extends FlowNode

object FlowNode {
  def source(dataFrameName: String): SourceNode = {
    SourceNode(dataFrameName)
  }

  def ofTransformer11(transformer11: Transformer11): Transformer11 = {
    transformer11
  }

  def ofTransformer21(transformer21: Transformer21): Transformer21 = {
    transformer21
  }

  def ofScalaFunction(func: DataFrame => DataFrame): Transformer11 = {
    (dataFrame: DataFrame) => func(dataFrame)
  }

  def stocked(functionId: String, args: Map[String, String] = Map.empty): RepositoryNode = {
    RepositoryNode(functionId, args)
  }

}