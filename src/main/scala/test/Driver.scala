package test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val client: Socket = new Socket("localhost", 9999)

    val out: OutputStream = client.getOutputStream
    val objOut: ObjectOutputStream = new ObjectOutputStream(out)
    val task = new Task
    objOut.writeObject(task)
    objOut.flush()
    objOut.close()

    client.close()
    println("客户端数据发送完毕！")
  }
}
