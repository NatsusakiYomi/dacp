package link.rdcn.dacp.optree.fifo

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.CreateContainerResponse
import com.github.dockerjava.api.model._
import com.github.dockerjava.core.command.ExecStartResultCallback
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}

import scala.collection.JavaConverters._

object DockerExecute {
  object ContainerState extends Enumeration {
    type ContainerState = Value
    val NotFound, Stopped, Running = Value
  }

  /**
   * 检查指定名称的容器是否正在运行
   * @param containerName 容器名称（例如 "jyg-container"）
   * @return Boolean 是否运行中
   */
  def isContainerRunning(containerName: String): Boolean =
  {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    try {
      // 列出所有容器（包括未运行的）
      val containers: List[Container] = dockerClient.listContainersCmd()
        .withShowAll(true) // 显示所有容器（包括已停止的）
        .exec()
        .asScala
        .toList

      // 按名称过滤并检查状态
      containers.exists { container =>
        container.getNames.contains(s"/$containerName") &&
          container.getState.equalsIgnoreCase("running")
      }
    } finally {
      dockerClient.close()
    }
  }

  /**
   * 启动一个交互式容器，并挂载目录
   *
   * @param hostDir     宿主机目录（如 `/data2/work/ncdc/faird/temp`）
   * @param containerDir 容器内挂载目录（如 `/mnt/data`）
   * @param containerName 容器名称（如 `jyg-container`）
   * @param imageName    镜像名称（如 `registry.cn-hangzhou.aliyuncs.com/cnic-piflow/siltdam-jyg:latest`）
   * @return 容器ID
   */
    def startContainer(
                        hostDir: String,
                        containerDir: String,
                        containerName: String,
                        imageName: String
                      ): String = {
      // 1. 配置 Docker 客户端
      val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
      val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

      try {
        // 2. 创建容器（挂载目录 + 交互式终端）
        val container: CreateContainerResponse = dockerClient.createContainerCmd(imageName)
          .withName(containerName)
          .withBinds(new Bind(hostDir, new Volume(containerDir))) // 挂载目录
          .withTty(true)    // 相当于 -t
          .withStdinOpen(true) // 相当于 -i
          .exec()

        // 3. 启动容器
        dockerClient.startContainerCmd(container.getId).exec()
        println(s"容器已启动，ID: ${container.getId}")

        container.getId
      } finally {
        dockerClient.close()
      }
    }

  // Array("python","op1.py")
  def nonInteractiveExec(command: Array[String], containerId: String): String = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    //    val containerId = "jyg-container"
    //    val command = Array("python", "/mnt/data/script.py")

    try {
      val execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
        .withCmd(command: _*)
        .withAttachStdin(true)   // 相当于 -i 参数
        .withAttachStdout(true)
        .withAttachStderr(true)
        .exec()

      val output = new StringBuilder
      val callback = new ExecStartResultCallback() {
        override def onNext(frame: Frame): Unit = {
          frame.getStreamType match {
            case StreamType.STDOUT | StreamType.STDERR =>
              output.append(new String(frame.getPayload))
            case _ => // 忽略其他类型
          }
          super.onNext(frame)
        }
      }

      dockerClient.execStartCmd(execCreateCmdResponse.getId)
        .withDetach(false)
        .withTty(false)  // 禁用TTY (相当于去掉 -t 参数)
        .exec(callback)
        .awaitCompletion()

      println("命令执行结果:")
      println(output.toString())
      output.toString()

    } finally {
      dockerClient.close()
    }
  }

  def getContainerState(containerName: String): ContainerState.Value = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    try {
      val containers: List[Container] = dockerClient.listContainersCmd()
        .withShowAll(true) // 显示所有容器
        .exec()
        .asScala
        .toList

      val foundContainer = containers.find { container =>
        container.getNames.contains(s"/$containerName")
      }

      foundContainer match {
        case Some(container) =>
          if (container.getState.equalsIgnoreCase("running")) {
            ContainerState.Running
          } else {
            ContainerState.Stopped
          }
        case None =>
          ContainerState.NotFound
      }
    } finally {
      dockerClient.close()
    }
  }

  // 启动现有容器
  def startExistingContainer(containerName: String): Unit = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()
    try {
      // 您需要先通过名字找到 ID
      val containerId = dockerClient.listContainersCmd().withShowAll(true).exec().asScala
        .find(_.getNames.contains(s"/$containerName"))
        .map(_.getId)
        .getOrElse(throw new NoSuchElementException(s"Container $containerName not found"))

      dockerClient.startContainerCmd(containerId).exec()
      println(s"已启动现有容器: $containerName (ID: $containerId)")
    } finally {
      dockerClient.close()
    }
  }

}


