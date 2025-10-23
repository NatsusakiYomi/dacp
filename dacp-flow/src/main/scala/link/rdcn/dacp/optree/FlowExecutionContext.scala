package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.dacp.recipe.FifoFileBundleFlowNode
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 16:00
 * @Modified By:
 */
trait FlowExecutionContext extends link.rdcn.operation.ExecutionContext {

  private[this] val asyncResults = new ConcurrentHashMap[TransformOp, Future[DataFrame]]()
  private[this] val asyncResultsList =
    new ConcurrentHashMap[TransformOp, ArrayBuffer[Thread]]()

  def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame],
                          thread: Thread): Unit = {
    asyncResults.put(transformOp, future)
    val parentKeyOpt = asyncResultsList.keys().asScala.find { key =>
      key.asInstanceOf[TransformerNode].contain(transformOp.asInstanceOf[TransformerNode])
    }
    parentKeyOpt match {
      case Some(key) =>
        println(s"TransformOp $transformOp 找到了父节点 $key，将线程加入列表")
        asyncResultsList.get(key).append(thread)

        future.onComplete {
          case Success(df) =>
            println("===========transform运行结束==============")
            transformOp.asInstanceOf[TransformerNode].transformFunctionWrapper
              .asInstanceOf[FifoFileBundleFlowNode].outputFilePath.foreach(println)
            println("===========transform运行结束==============")
          case Failure(e) =>
            asyncResults.remove(transformOp) // 从 future 列表中移除
            throw new Exception(s"TransformOp $transformOp failed", e)
        }

      case None =>
        println(s"TransformOp $transformOp 未找到父节点，将其注册为新的根节点")
        val arr = new ArrayBuffer[Thread]()
        arr.append(thread)
        asyncResultsList.put(transformOp, arr)

        future.onComplete {
          case Success(df) =>
            println(s"recipe $transformOp 完成释放资源")
            transformOp.asInstanceOf[TransformerNode].release()
          case Failure(e) =>
            asyncResultsList.remove(transformOp)
            asyncResults.remove(transformOp)
            throw new Exception(s"TransformOp $transformOp failed", e)
        }
    }
//    asyncResultsList.keys().asScala.foreach(key => {
//      if(key.asInstanceOf[TransformerNode].contain(transformOp.asInstanceOf[TransformerNode])){
//        asyncResultsList.get(key).append(thread)
//        future.onComplete{
//          case Success(df) => println("===========transform运行结束==============")
//            transformOp.asInstanceOf[TransformerNode].transformFunctionWrapper
//          .asInstanceOf[FifoFileBundleFlowNode].outputFilePath.foreach(println)
//            println("===========transform运行结束==============")
//          case Failure(e) =>
//            asyncResults.remove(transformOp)
//            throw new Exception(s"TransformOp $transformOp failed", e)
//        }
//      }else {
//        val arr = new ArrayBuffer[Thread]()
//        arr.append(thread)
//        asyncResultsList.put(transformOp, arr)
//        future.onComplete {
//          case Success(df) =>
//            println(s"recipe 完成释放资源")
//            transformOp.asInstanceOf[TransformerNode].release()
//          case Failure(e) =>
//            asyncResultsList.remove(transformOp)
//            asyncResults.remove(transformOp)
//            throw new Exception(s"TransformOp $transformOp failed", e)
//        }
//      }
//    })
  }

  def getAsyncResult(transformOp: TransformOp): Option[Future[DataFrame]] = {
    Option(asyncResults.get(transformOp))
  }

  def getAsyncThreads(transformOp: TransformOp): Option[ArrayBuffer[Thread]] = {
    Option(asyncResultsList.get(transformOp))
  }

  val fairdHome: String

  def pythonHome: String

  def isAsyncEnabled: Boolean = false

  def loadRemoteDataFrame(baseUrl: String, path:String, credentials: Credentials): Option[DataFrame]

  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}
