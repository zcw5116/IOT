package scalaCase

/**
  * Created by slview on 17-6-16.
  *
  */


object Currying {

  // 尾递归
  def sum(f: Int => Int)(a: Int)(b: Int): Int = {
    @annotation.tailrec
    def loop(n: Int, acc: Int): Int = {
      println("n:" + n + ",acc:" + acc)
      if (n > b) {
        acc
      }
      else {
        println("n:" + n + ",acc:" + acc)
        loop(n + 1, acc + f(n))
      }
    }
    loop(a, 0)
  }

  def main(args: Array[String]): Unit = {
    sum(x => x )(1)(5)
    println("###############")
    sum(x => x * x)(1)(5)
    println("###############")
    sum(x => x * x * x)(1)(5)
    println("###############")

    // 柯里化
    val sumSquare = sum(x => x*x)_
    sumSquare(2)(5)

  }


}
