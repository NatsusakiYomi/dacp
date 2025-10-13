package link.rdcn.dacp.optree

import link.rdcn.operation.SourceOp
import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.Credentials
import org.json.{JSONArray, JSONObject}
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.duration._

class TransformTreeTest {
  private val JYG_CONTAINER_NAME = "jyg-container"
  private val JYG_IMAGE = "registry.cn-hangzhou.aliyuncs.com/cnic-piflow/siltdam-jyg:latest"

  private val HOST_DIR = "/data2/work/ncdc/faird/temp"
  private val CONTAINER_DIR = "/mnt/data"
  private val OP_GULLY_DIR = Paths.get(CONTAINER_DIR, "op1", "op1.py").toString
  private val OP_GULLY_OUTPUT = Paths.get(HOST_DIR, "op1", "gully_slop_fifo.csv").toString
  private val OP_HDYRO_DIR = Paths.get(CONTAINER_DIR, "op3", "op3.py").toString
  private val OP_HDYRO_OUTPUT = Paths.get(HOST_DIR, "op3", "suscep_hdyro_fifo.csv").toString
  private val OP_GEOTRANS_DIR = Paths.get(CONTAINER_DIR, "op2", "op2.py").toString
  private val OP_GEOTRANS_OUTPUT = Paths.get(HOST_DIR, "op2", "labels").toString
  private val OP_GEOENTROPY_DIR = Paths.get(CONTAINER_DIR, "op4", "op4.py").toString
  private val OP_GEOENTROPY_OUTPUT = Paths.get(HOST_DIR, "op4", "geo_entropy_fifo.csv").toString
  private val OP_OVERLAP_DIR = Paths.get(CONTAINER_DIR, "op5", "op5.py").toString
  private val OP_OVERLAP_OUTPUT = Paths.get(HOST_DIR, "op5", "DamDetect_select2_fifo.csv").toString

  private val ctx = new FlowExecutionContext {
    override val isAsyncEnabled = true

    override val fairdHome: String = ""

    override def pythonHome: String = ""

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
      Some(DefaultDataFrame(StructType.empty, Iterator.empty))
    }

    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))

    override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = ???
  }

  val joGully = new JSONObject()
  val commandArrayGully = new JSONArray()
  commandArrayGully.put("python")
  commandArrayGully.put(OP_GULLY_DIR)
  joGully.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
  joGully.put("command", commandArrayGully)
  joGully.put("outPutFilePath", new JSONArray().put(OP_GULLY_OUTPUT))
  joGully.put("containerName", JYG_CONTAINER_NAME)

  val joEntropy = new JSONObject()
  val commandArrayEntropy = new JSONArray()
  commandArrayEntropy.put("python")
  commandArrayEntropy.put(OP_GEOENTROPY_DIR)
  joEntropy.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
  joEntropy.put("command", commandArrayEntropy)
  joEntropy.put("outPutFilePath", new JSONArray().put(OP_GEOENTROPY_OUTPUT))
  joEntropy.put("containerName", JYG_CONTAINER_NAME)

  val joTrans = new JSONObject()
  val commandArrayTrans = new JSONArray()
  commandArrayTrans.put("python")
  commandArrayTrans.put(OP_GEOTRANS_DIR)
  val paths = new JSONArray()
  getFileNamesInFolder(OP_GEOTRANS_OUTPUT).foreach(element =>
    paths.put(Paths.get(HOST_DIR, "op2", "output",element).toString)
  )
  joTrans.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
  joTrans.put("command", commandArrayTrans)
  joTrans.put("outPutFilePath", paths)
  joTrans.put("containerName", JYG_CONTAINER_NAME)

  val joHydro = new JSONObject()
  val commandArrayHydro = new JSONArray()
  commandArrayHydro.put("python")
  commandArrayHydro.put(OP_HDYRO_DIR)
  joHydro.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
  joHydro.put("command", commandArrayHydro)
  joHydro.put("outPutFilePath", new JSONArray().put(OP_HDYRO_OUTPUT))
  joHydro.put("containerName", JYG_CONTAINER_NAME)

  val joOverlap = new JSONObject()
  val commandArrayOverlap = new JSONArray()
  commandArrayOverlap.put("python")
  commandArrayOverlap.put(OP_OVERLAP_DIR)
  joOverlap.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
  joOverlap.put("command", commandArrayOverlap)
  joOverlap.put("outPutFilePath", new JSONArray().put(OP_OVERLAP_OUTPUT))
  joOverlap.put("containerName", JYG_CONTAINER_NAME)

  val transformTreeGully = TransformerNode(TransformFunctionWrapper.fromJsonObject(joGully).asInstanceOf[FileRepositoryBundle], SourceOp(""))
  val transformTreeEntropy = TransformerNode(TransformFunctionWrapper.fromJsonObject(joEntropy).asInstanceOf[FileRepositoryBundle], SourceOp(""))
  val transformTreeHydro = TransformerNode(TransformFunctionWrapper.fromJsonObject(joHydro).asInstanceOf[FileRepositoryBundle], transformTreeGully, transformTreeEntropy)
  val transformTreeTrans = TransformerNode(TransformFunctionWrapper.fromJsonObject(joTrans).asInstanceOf[FileRepositoryBundle], SourceOp(""))
  val transformTreeOverlap = TransformerNode(TransformFunctionWrapper.fromJsonObject(joOverlap).asInstanceOf[FileRepositoryBundle], transformTreeHydro, transformTreeTrans)

  @Test
  def TransformTreeSimpleFIFOFlowTest(): Unit = {
    println("transformTree execution started...")
//    transformTreeGully.execute(ctx)
//    Await.result(ctx.getAsyncResult(transformTreeGully).get,Duration.Inf)
    transformTreeHydro.execute(ctx)
    Await.result(ctx.getAsyncResult(transformTreeHydro).get,Duration.Inf)
//    transformTreeOverlap.execute(ctx)
//    Await.result(ctx.getAsyncResult(transformTreeOverlap).get,Duration.Inf)// Assuming this returns Unit
    println("transformTree execution finished.")
  }

  def getFileNamesInFolder(folderPath: String): Seq[String] = {
    val folder = new File(folderPath)

    // 检查路径是否存在并且确实是一个文件夹
    if (folder.exists && folder.isDirectory) {
      // listFiles 返回一个 Array[File]，可能为 null，所以用 Option 包装更安全
      Option(folder.listFiles)
        .map(_.toSeq) // 转换为 Seq
        .getOrElse(Seq.empty) // 如果 listFiles 返回 null，则提供一个空 Seq
        .filter(_.isFile) // 只保留文件
        .map(_.getName) // 获取文件名
    } else {
      println(s"警告: 路径 '$folderPath' 不存在或不是一个文件夹。")
      Seq.empty[String] // 如果路径无效，返回空 Seq
    }
  }
}
