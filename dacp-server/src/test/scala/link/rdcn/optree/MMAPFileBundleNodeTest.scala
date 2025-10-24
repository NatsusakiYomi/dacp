package link.rdcn.optree

import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.optree.fifo.DockerContainer
import link.rdcn.dacp.recipe._
import link.rdcn.dacp.server.DacpServer
import link.rdcn.server.{AuthorProviderTest, DataProviderTest, DataReceiverTest}
import link.rdcn.user.UsernamePassword
import org.junit.jupiter.api.{BeforeAll, Test}

object MMAPFileBundleNodeTest{

  var server: DacpServer = _

  @BeforeAll
  def serverStart(): Unit = {
    server = new DacpServer(new DataProviderTest, new DataReceiverTest, new AuthorProviderTest)
    server.start(new FairdConfig)
  }
}

class MMAPFileBundleNodeTest {
  @Test
  def MMAPtoFIFOMMAPTest(): Unit = {
    //输出MMAP未删除
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))

    val nodeGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.MMAP_FILE,
      FileType.MMAP_FILE
    )

    val nodeHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.FIFO_BUFFER,
      FileType.MMAP_FILE
    )

    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")

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
    val result = dacpClient.execute(recipe)
    result.single().foreach(println)

  }

  @Test
  def MMAPtoFIFOFIFOTest(): Unit = {
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))

    val nodeGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.MMAP_FILE,
      FileType.MMAP_FILE
    )

    val nodeHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.FIFO_BUFFER,
      FileType.FIFO_BUFFER
    )

    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")

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
    val result = dacpClient.execute(recipe)
    result.single().foreach(println)

  }

  @Test
  def FIFOtoMMAPMMAPTest(): Unit = {
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))

    val nodeGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.MMAP_FILE,
      FileType.FIFO_BUFFER
    )

    val nodeHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.MMAP_FILE,
      FileType.MMAP_FILE
    )

    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")

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
    val result = dacpClient.execute(recipe)
    result.single().foreach(println)

  }

  @Test
  def FIFOtoMMAPFIFOTest(): Unit = {
    //输出FIFO未删除
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))

    val nodeGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.FIFO_BUFFER,
      FileType.FIFO_BUFFER
    )

    val nodeHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container"),
      FileType.MMAP_FILE,
      FileType.FIFO_BUFFER
    )

    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")

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
    val result = dacpClient.execute(recipe)
    result.single().foreach(println)

  }

}
