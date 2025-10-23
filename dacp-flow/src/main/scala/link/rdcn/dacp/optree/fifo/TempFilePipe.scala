package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._

import java.io._

case class TempFilePipe(file: File) extends FilePipe(file) {

  override def create(): Unit = {
    if (file.exists()) {
      Runtime.getRuntime.exec(Array("rm", "-rf", file.getAbsolutePath))
    }
      Runtime.getRuntime.exec(Array("touch", file.getAbsolutePath)).waitFor()
  }

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


  override def dataFrame(): DataFrame =
    DefaultDataFrame(StructType.empty.add("content", StringType),
      ClosableIterator(read().map(str => Row.fromSeq(Seq(str))))())
}

object TempFilePipe {

  def fromFilePath(path: String): TempFilePipe = {
    val pipe = new TempFilePipe(new File(path))
    pipe.create()
    pipe
  }

  def fromFile(file: File): TempFilePipe = {
    val pipe = new TempFilePipe(file)
    pipe.create()
    pipe
  }

}