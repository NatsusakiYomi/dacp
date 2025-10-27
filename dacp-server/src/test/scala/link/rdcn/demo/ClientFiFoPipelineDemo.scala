package link.rdcn.demo

import cn.cnic.operatordownload.client.OperatorClient
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.optree.fifo.DockerContainer
import link.rdcn.dacp.recipe.{FifoFileBundleFlowNode, FifoFileFlowNode, Flow}
import link.rdcn.user.UsernamePassword
import org.json.{JSONArray, JSONObject}

import java.io.File
import java.nio.file.Files

object ClientFiFoPipelineDemo {
  def main(args: Array[String]): Unit = {
    // 创建算子客户端实例，指向算子下载服务的地址
    // token: 用户认证,默认填null先
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    //        OperatorClient client = OperatorClient.connect("http://localhost:8088", null);//        OperatorClient client = OperatorClient.connect("http://localhost:8088", null);

    try {
      // 上传算子
      println("=== 上传算子 ===")
      // 创建测试文件
      val tempFile = Files.createTempFile("test-operator", ".whl")
      Files.write(tempFile, "this is a test operator file".getBytes)
      val requirementsFile = Files.createTempFile("requirements", ".txt")
      Files.write(requirementsFile, "numpy==1.21.0\npandas>=1.3.0".getBytes)
      val paramFile = Files.createTempFile("param", ".json")
      Files.write(paramFile, "[{\"demo\": \"/path/to/input.csv\", \"name\": \"input_file\", \"number\": 1, \"fileType\": \"RAM_FILE\", \"paramType\": \"输入文件\", \"paramDescription\": \"输入数据文件路径\"}, {\"demo\": \"/path/to/output.json\", \"name\": \"output_file\", \"number\": 2, \"fileType\": \"RAM_FILE\", \"paramType\": \"输出文件\", \"paramDescription\": \"/mnt/data/temp2/gully_slop_fifo.csv\"}]".getBytes)

      val uploadResult = client.uploadOperator(tempFile.toFile, // file 算子文件
        requirementsFile.toFile, // requirementsFile 依赖文件
        paramFile.toFile, // paramFile 参数文件
        "hydro_susceptibility", // name 算子名称
        "1.0.4", // version 版本号
        "测试算子描述", // description 描述
        "测试作者", // author 作者
        "/image/testClient", // categoryPath 目录路径
        "PYTHON_IMAGE", // targetType 上传目标类型
        "python-script", // type 算子类型
        "python", // language 编程语言
        "pandas", // framework 框架
        "python /mnt/data/temp2/gully_slop.py", // command 执行命令
        "帮助信息", // help 帮助信息
        "test@example.com", // email 邮箱
        "faird/main.py", // entryPoint 入口点
        "Test Operator" // nameEn 英文名称
      )
      println("算子上传成功: " + uploadResult)

      // 清理临时文件
      Files.delete(tempFile)
      Files.delete(requirementsFile)
      Files.delete(paramFile)

      // 根据名称和版本查询算子信息
      println("\n=== 查询算子信息 ===")
      val gullyOperatorInfo = new JSONObject(client.getOperatorByNameAndVersion("gully_slop", "1.0.4"))
      val hydroOperatorInfo = new JSONObject(client.getOperatorByNameAndVersion("hydro_susceptibility", "1.0.0"))

      println("算子信息: " + gullyOperatorInfo)
      println("算子信息: " + hydroOperatorInfo)

      // 通过获得的算子镜像执行算子流水线
      // 创建DACP客户端
      val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("admin@instdb.cn", "admin001"))

      // 创建算子节点
      // 获得挂载路径
      val paramInfos = new JSONArray(gullyOperatorInfo.getJSONObject("data").getString("paramInfos"))
      // val path = new File(new JSONObject(paramInfos.getJSONObject(paramInfos.length()-1)).getString("paramDescription")).getParent
      // 通过算子信息获得算子镜像
      val gullyImage = gullyOperatorInfo.getJSONObject("data").getString("nexusUrl")
      // 指定创建以FIFO文件作为中间数据进行传输算子节点
      val nodeGullySlop = FifoFileBundleFlowNode(
        // 以容器中算子执行
        gullyOperatorInfo.getJSONObject("data").getString("command").split(" "),
        // 以指定的本地算子挂载到容器执行
        // Seq("python", "/mnt/data/temp2/gully_slop.py"),
        Seq(""),
        Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
        // 指定容器名称、挂载的宿主路径、容器内路径以及镜像地址
        DockerContainer("new-jyg-container-2", Some("/data2/work/ncdc/faird/temp"), Some("/mnt/data"), Some(gullyImage))
      )

      val hydroImage = hydroOperatorInfo.getJSONObject("data").getString("nexusUrl")
      val nodeHydro = FifoFileBundleFlowNode(
        hydroOperatorInfo.getJSONObject("data").getString("command").split(" "),
        // Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
        Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
        Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
        DockerContainer("new-jyg-container-2", Some("/data2/work/ncdc/faird/temp"), Some("/mnt/data"), Some(hydroImage))
      )

      // 结果输出节点
      val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")

      // 构建DAG流水线
      val recipe = Flow(
        Map(
          "A" -> nodeGullySlop,
          "B" -> nodeHydro,
          "C" -> fifoFileNode

        ),
        Map(
          "A" -> Seq("B"),
          "B" -> Seq("C")
        )
      )

      // 执行流水线
      val result = dacpClient.execute(recipe)

      //输出运行结果
      result.single().foreach(println)

    } catch {
      case e: Exception =>
        System.err.println("操作失败: " + e.getMessage)
        e.printStackTrace()
    }
  }

}


