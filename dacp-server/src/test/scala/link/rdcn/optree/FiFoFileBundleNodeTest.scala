package link.rdcn.optree

import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.recipe._
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.optree.{FiFoFileNode, FileRepositoryBundle, TransformerNode}
import link.rdcn.dacp.optree.fifo.{DockerContainer, DockerExecute}
import link.rdcn.dacp.server.DacpServer

import link.rdcn.server.{AuthorProviderTest, DataProviderTest, DataReceiverTest}
import link.rdcn.user.{Credentials, UsernamePassword}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object FiFoFileBundleNodeTest{

  var server: DacpServer = _

  @BeforeAll
  def serverStart(): Unit = {
    server = new DacpServer(new DataProviderTest, new DataReceiverTest, new AuthorProviderTest)
    server.start(new FairdConfig)
  }
}

class FiFoFileBundleNodeTest {

  @Test
  def gully_hydro(): Unit = {
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))

    val nodeGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val nodeHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container")
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
  def erosionDataPipelineTest(): Unit = {
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))
    val fileRepositoryGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val fileRepositoryHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val fileRepositoryGeoTransMain = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/geotrans_main.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/output/13209_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6348.txt"),
      DockerContainer("jyg-container")
    )


    val fileRepositorySelect = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/overlap_dam_select.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6348.txt"
      ),
      Seq("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val transFifo = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv")

    val recipe = Flow(
      Map(
        "gully" -> fileRepositoryGullySlop,
        "hydro" -> fileRepositoryHydro,
        "transmain" -> fileRepositoryGeoTransMain,
        "select" -> fileRepositorySelect,
        "output" -> transFifo
      ),
      Map(
        "gully" -> Seq("hydro"),
        "hydro" -> Seq("select"),
        "transmain" -> Seq("select"),
        "select" -> Seq("output")
      )
    )
    val result = dacpClient.execute(recipe)
    println("---------Execution Result----------")
    result.single().foreach(println)
  }

  @Test
  def erosionDataPipelineWithEntropyTest(): Unit = {
    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))
    val fileRepositoryGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/gully_slop.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val fileRepositoryEntropy = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/geo_entropy.py","/a.csv","/b.csv"),
      Seq(),
      Seq("/data2/work/ncdc/faird/temp/temp2/geo_entropy_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val fileRepositoryHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility_fifo.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv",
      "/data2/work/ncdc/faird/temp/temp2/geo_entropy_fifo.csv"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val fileRepositoryGeoTransMain = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/geotrans_main.py"),
      Seq(""),
      Seq("/data2/work/ncdc/faird/temp/temp2/output/13209_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6348.txt"),
      DockerContainer("jyg-container")
    )


    val fileRepositorySelect = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/overlap_dam_select.py"),
      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6348.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13209_6347.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13210_6346.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13211_6345.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13212_6344.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13213_6343.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13214_6342.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13215_6341.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13216_6340.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6339.txt",
        "/data2/work/ncdc/faird/temp/temp2/output/13217_6348.txt"
      ),
      Seq("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv"),
      DockerContainer("jyg-container")
    )

    val transFifo = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv")

    val recipe = Flow(
      Map(
        "gully" -> fileRepositoryGullySlop,
        "geo" -> fileRepositoryEntropy,
        "hydro" -> fileRepositoryHydro,
        "transmain" -> fileRepositoryGeoTransMain,
        "select" -> fileRepositorySelect,
        "output" -> transFifo
      ),
      Map(
        "gully" -> Seq("hydro"),
        "geo" ->  Seq("hydro"),
        "hydro" -> Seq("select"),
        "transmain" -> Seq("select"),
        "select" -> Seq("output")
      )
    )
    val result = dacpClient.execute(recipe)
    result.single().foreach(println)
  }

}
