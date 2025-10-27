package link.rdcn.dacp.optree.fifo

import org.json.JSONObject

case class DockerContainer(
                            containerName: String,
                            hostPath: Option[String] = None,
                            containerPath: Option[String] = None,
                            imageName: Option[String] = None
                          ){
  def start(): String = {
    DockerExecute.getContainerState(containerName) match {
      case DockerExecute.ContainerState.Running =>
        println(s"容器 $containerName 已经在运行。")
        containerName
      case DockerExecute.ContainerState.Stopped =>
        println(s"容器 $containerName 存在但已停止，正在启动...")
        DockerExecute.startExistingContainer(containerName) // 只启动，不创建
        containerName
      case DockerExecute.ContainerState.NotFound =>
        println(s"容器 $containerName 未找到，正在创建新容器...")
        DockerExecute.startContainer(hostPath.get, containerPath.get, containerName, imageName.get)
    }
  }

  def toJson(): JSONObject = {
    val jo = new JSONObject
    jo.put("containerName", containerName)
    hostPath.map(jo.put("hostPath", _))
    containerPath.map(jo.put("containerPath", _))
    imageName.map(jo.put("imageName", _))
    jo
  }
}

object DockerContainer{
  def fromJson(jo: JSONObject): DockerContainer = {
    DockerContainer(jo.getString("containerName"),
      Option(jo.optString("hostPath", null)),
      Option(jo.optString("containerPath", null)),
      Option(jo.optString("imageName", null))
    )
  }
}
