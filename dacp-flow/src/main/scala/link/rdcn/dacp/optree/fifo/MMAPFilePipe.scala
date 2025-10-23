package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._

import java.io.{BufferedReader, ByteArrayOutputStream, File, FileReader, FileWriter, PrintWriter, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.{BufferUnderflowException, MappedByteBuffer}
import scala.collection.mutable.ListBuffer

case class MMAPFilePipe(file: File) extends FilePipe(file) {
  val fileSize = 1024 * 1024 * 100L
  var fileChannel: FileChannel = null
  val charset: Charset = StandardCharsets.UTF_8
  val END_OF_MESSAGES: Int = -1 // 结束标记
  val INT_BYTES: Int = 4        // 4 字节用于存储长度
  var mappedBuffer: MappedByteBuffer = null

  override def create(): Unit = {
    if (file.exists()) {
      Runtime.getRuntime.exec(Array("rm", "-rf", file.getAbsolutePath))
    }
    val parentDir = file.getParentFile
    if (parentDir != null && parentDir.exists()) {
      val raf = new RandomAccessFile(file.getAbsolutePath, "rw")
      raf.setLength(fileSize)
      fileChannel = raf.getChannel
      mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
    }
  }


  override def dataFrame(): DataFrame =
    DefaultDataFrame(StructType.empty.add("content", StringType),
      ClosableIterator(read().map(str => Row.fromSeq(Seq(str))))())

  def write(messages: Iterator[String]): Unit = {
    val writer = new PrintWriter(new FileWriter(file))
    try {
      messages.foreach { message =>
        writer.println(message)
        writer.flush()
      }
    } finally {
      writer.close()
    }
  }

  def read(): ClosableIterator[String] = {
    val iter = new Iterator[String] {
      private val reader = new BufferedReader(new FileReader(file))
      private var nextLine: String = reader.readLine()
      private var isClosed = false

      override def hasNext: Boolean = {
        if (nextLine == null && !isClosed) {
          reader.close()
          isClosed = true
          false
        } else {
          nextLine != null
        }
      }

      override def next(): String = {
        if (!hasNext) throw new NoSuchElementException("No more lines")
        val current = nextLine
        nextLine = reader.readLine()
        if (nextLine == null) {
          reader.close()
          isClosed = true
        }
        current
      }
    }
    ClosableIterator(iter)(() => {})
  }

}

object MMAPFilePipe {

  def fromFilePath(path: String): MMAPFilePipe = {
    val pipe = new MMAPFilePipe(new File(path))
    pipe.create()
    pipe
  }

  def fromFile(file: File): MMAPFilePipe = {
    val pipe = new MMAPFilePipe(file)
    pipe.create()
    pipe
  }

}