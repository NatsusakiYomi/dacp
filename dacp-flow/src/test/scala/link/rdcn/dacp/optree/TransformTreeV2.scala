//package link.rdcn.dacp.optree
//
//import link.rdcn.dacp.optree.fifo.DockerExecute
//import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
//import link.rdcn.user.Credentials
//import org.junit.jupiter.api.{AfterAll, Test}
//import link.rdcn.dacp.optree.fifo.DockerContainer
//
//object TransformTreeV2{
//
//  @AfterAll
//  def delete(): Unit = {
//    DockerExecute.nonInteractiveExec(Array("rm","-rf",
//      "/mnt/data/temp2/gully_slop_fifo.csv",
//      "/mnt/data/temp2/suscep_hdyro_fifo.csv",
//      "/mnt/data/temp2/DamDetect_select_fifo.csv",
//      "/mnt/data/temp2/output/13209_6339.txt",
//      "/mnt/data/temp2/output/13209_6348.txt",
//      "/mnt/data/temp2/output/13210_6347.txt",
//      "/mnt/data/temp2/output/13211_6346.txt",
//      "/mnt/data/temp2/output/13212_6345.txt",
//      "/mnt/data/temp2/output/13213_6344.txt",
//      "/mnt/data/temp2/output/13214_6343.txt",
//      "/mnt/data/temp2/output/13215_6342.txt",
//      "/mnt/data/temp2/output/13216_6341.txt",
//      "/mnt/data/temp2/output/13217_6340.txt",
//      "/mnt/data/temp2/output/13209_6340.txt",
//      "/mnt/data/temp2/output/13210_6339.txt",
//      "/mnt/data/temp2/output/13210_6348.txt",
//      "/mnt/data/temp2/output/13211_6347.txt",
//      "/mnt/data/temp2/output/13212_6346.txt",
//      "/mnt/data/temp2/output/13213_6345.txt",
//      "/mnt/data/temp2/output/13214_6344.txt",
//      "/mnt/data/temp2/output/13215_6343.txt",
//      "/mnt/data/temp2/output/13216_6342.txt",
//      "/mnt/data/temp2/output/13217_6341.txt",
//      "/mnt/data/temp2/output/13209_6341.txt",
//      "/mnt/data/temp2/output/13210_6340.txt",
//      "/mnt/data/temp2/output/13211_6339.txt",
//      "/mnt/data/temp2/output/13211_6348.txt",
//      "/mnt/data/temp2/output/13212_6347.txt",
//      "/mnt/data/temp2/output/13213_6346.txt",
//      "/mnt/data/temp2/output/13214_6345.txt",
//      "/mnt/data/temp2/output/13215_6344.txt",
//      "/mnt/data/temp2/output/13216_6343.txt",
//      "/mnt/data/temp2/output/13217_6342.txt",
//      "/mnt/data/temp2/output/13209_6342.txt",
//      "/mnt/data/temp2/output/13210_6341.txt",
//      "/mnt/data/temp2/output/13211_6340.txt",
//      "/mnt/data/temp2/output/13212_6339.txt",
//      "/mnt/data/temp2/output/13212_6348.txt",
//      "/mnt/data/temp2/output/13213_6347.txt",
//      "/mnt/data/temp2/output/13214_6346.txt",
//      "/mnt/data/temp2/output/13215_6345.txt",
//      "/mnt/data/temp2/output/13216_6344.txt",
//      "/mnt/data/temp2/output/13217_6343.txt",
//      "/mnt/data/temp2/output/13209_6343.txt",
//      "/mnt/data/temp2/output/13210_6342.txt",
//      "/mnt/data/temp2/output/13211_6341.txt",
//      "/mnt/data/temp2/output/13212_6340.txt",
//      "/mnt/data/temp2/output/13213_6339.txt",
//      "/mnt/data/temp2/output/13213_6348.txt",
//      "/mnt/data/temp2/output/13214_6347.txt",
//      "/mnt/data/temp2/output/13215_6346.txt",
//      "/mnt/data/temp2/output/13216_6345.txt",
//      "/mnt/data/temp2/output/13217_6344.txt",
//      "/mnt/data/temp2/output/13209_6344.txt",
//      "/mnt/data/temp2/output/13210_6343.txt",
//      "/mnt/data/temp2/output/13211_6342.txt",
//      "/mnt/data/temp2/output/13212_6341.txt",
//      "/mnt/data/temp2/output/13213_6340.txt",
//      "/mnt/data/temp2/output/13214_6339.txt",
//      "/mnt/data/temp2/output/13214_6348.txt",
//      "/mnt/data/temp2/output/13215_6347.txt",
//      "/mnt/data/temp2/output/13216_6346.txt",
//      "/mnt/data/temp2/output/13217_6345.txt",
//      "/mnt/data/temp2/output/13209_6345.txt",
//      "/mnt/data/temp2/output/13210_6344.txt",
//      "/mnt/data/temp2/output/13211_6343.txt",
//      "/mnt/data/temp2/output/13212_6342.txt",
//      "/mnt/data/temp2/output/13213_6341.txt",
//      "/mnt/data/temp2/output/13214_6340.txt",
//      "/mnt/data/temp2/output/13215_6339.txt",
//      "/mnt/data/temp2/output/13215_6348.txt",
//      "/mnt/data/temp2/output/13216_6347.txt",
//      "/mnt/data/temp2/output/13217_6346.txt",
//      "/mnt/data/temp2/output/13209_6346.txt",
//      "/mnt/data/temp2/output/13210_6345.txt",
//      "/mnt/data/temp2/output/13211_6344.txt",
//      "/mnt/data/temp2/output/13212_6343.txt",
//      "/mnt/data/temp2/output/13213_6342.txt",
//      "/mnt/data/temp2/output/13214_6341.txt",
//      "/mnt/data/temp2/output/13215_6340.txt",
//      "/mnt/data/temp2/output/13216_6339.txt",
//      "/mnt/data/temp2/output/13216_6348.txt",
//      "/mnt/data/temp2/output/13217_6347.txt",
//      "/mnt/data/temp2/output/13209_6347.txt",
//      "/mnt/data/temp2/output/13210_6346.txt",
//      "/mnt/data/temp2/output/13211_6345.txt",
//      "/mnt/data/temp2/output/13212_6344.txt",
//      "/mnt/data/temp2/output/13213_6343.txt",
//      "/mnt/data/temp2/output/13214_6342.txt",
//      "/mnt/data/temp2/output/13215_6341.txt",
//      "/mnt/data/temp2/output/13216_6340.txt",
//      "/mnt/data/temp2/output/13217_6339.txt",
//      "/mnt/data/temp2/output/13217_6348.txt"
//    ),
//      "jyg-container")
//  }
//}
//
//class TransformTreeV2 {
//
//  private val ctx = new FlowExecutionContext {
//    override val isAsyncEnabled = true
//
//    override val fairdHome: String = ""
//
//    override def pythonHome: String = ""
//
//    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
//      Some(DefaultDataFrame(StructType.empty, Iterator.empty))
//    }
//
//    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))
//
//    override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = ???
//  }
//
//  // gully_slop -> hydro_susceptibility
//
//  @Test
//  def gully_hydro(): Unit = {
//    val fileRepositoryGullySlop = new FileRepositoryBundle(
//      Seq("python", "/mnt/data/temp2/gully_slop.py"),
//      Seq(""),
//      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
//      DockerContainer("jyg-container")
//    )
//
//    val fileRepositoryHydro = new FileRepositoryBundle(
//      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
//      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
//      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
//      DockerContainer("jyg-container")
//    )
//    val transOp1 = TransformerNode(fileRepositoryGullySlop, link.rdcn.operation.SourceOp(""))
//    val transOp2 = TransformerNode(fileRepositoryHydro, transOp1)
//
//    val transFifo = FiFoFileNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv", transOp2)
//
//    transFifo.execute(ctx).foreach(println)
//  }
//
//  @Test
//  def erosionDataPipelineTest(): Unit = {
//    val fileRepositoryGullySlop = new FileRepositoryBundle(
//      Seq("python", "/mnt/data/temp2/gully_slop.py"),
//      Seq(""),
//      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
//      DockerContainer("jyg-container")
//    )
//
//    val fileRepositoryHydro = new FileRepositoryBundle(
//      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
//      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
//      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
//      DockerContainer("jyg-container")
//    )
//    val transOp1 = TransformerNode(fileRepositoryGullySlop)
//    val transOp2 = TransformerNode(fileRepositoryHydro, transOp1)
//
//
//    val fileRepositoryGeoTransMain = FileRepositoryBundle(
//      Seq("python", "/mnt/data/temp2/geotrans_main.py"),
//      Seq(""),
//      Seq("/data2/work/ncdc/faird/temp/temp2/output/13209_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6348.txt"),
//      DockerContainer("jyg-container")
//    )
//    val transOpGeoTransMain = TransformerNode(fileRepositoryGeoTransMain, link.rdcn.operation.SourceOp(""))
//
//    val fileRepositorySelect = FileRepositoryBundle(
//      Seq("python", "/mnt/data/temp2/overlap_dam_select.py"),
//      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6348.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13209_6347.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13210_6346.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13211_6345.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13212_6344.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13213_6343.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13214_6342.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13215_6341.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13216_6340.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6339.txt",
//        "/data2/work/ncdc/faird/temp/temp2/output/13217_6348.txt"
//      ),
//      Seq("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv"),
//      DockerContainer("jyg-container")
//    )
//    val transOpSelect = TransformerNode(fileRepositorySelect, transOpGeoTransMain, transOp2)
//
//    val transFifo = FiFoFileNode("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv", transOpSelect)
//
//    transFifo.execute(ctx).foreach(println)
//
//  }
//
//
//}
