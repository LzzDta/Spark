package test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {
    val server: ServerSocket = new ServerSocket(9999)
    println("服务器启动，等待接收数据！")

    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val objIn: ObjectInputStream = new ObjectInputStream(in)
    val task: Task = objIn.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.compute()
    println("计算的结果为：" + ints)
    in.close()
    client.close()
    server.close()
  }
}
