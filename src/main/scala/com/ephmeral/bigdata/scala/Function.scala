package com.ephmeral.bigdata.scala

object Function {
  def main(args: Array[String]): Unit = {
    var arr = Array(1, 2, 3, 4);
    def addOne(elem: Int): Int = elem + 1
    println(arrayOp(arr, addOne _).mkString(", ")) // pass addOne also work
    println(arrayOp(arr, elem => elem * 2).mkString(", "))
    println(arrayOp(arr, _ * 3).mkString(", "))

    // simplify to anonymous function
    def func1(a: Int): String => (Char => Boolean) = {
      s => c => !(a == 0 && s == "" && c == '0')
    }
    println(func1(0)("")('0')) // false
    println(func1(1)("hello")('c')) // true

    // Currying
    def add(a: Int)(b: Int) : Int = a + b
    println(add(10)(20))
    val addFour = add(4) _
    println(addFour(3))

    // pass by value
    def f0(a: Int): Unit = {
      println("a: " + a)
      println("a: " + a)
    }
    f0(10)
    println("=======================")

    // pass by name, argument can be a code block that return to Int
    def f1(a: => Int): Unit = {
      println("a: " + a)
      println("a: " + a)
    }
    def f2(): Int = {
      println("call f2()")
      10
    }
    f1(10)
    println("===========f1(f2())============")
    f1(f2()) // pass by name, just replace a with f2(), then will call f2() twice
    println("=========f1({})==============")
    f1({
      println("code block") // print twice
      30
    })

    def factorial(x: BigInt) : BigInt = {
      if (x == 0) {
        1
      } else {
        x * factorial(x - 1)
      }
    }

    println("30! = " + factorial(30))
  }

  def arrayOp(arr: Array[Int], op: Int => Int) : Array[Int] = {
    for (elem <- arr) yield op(elem)
  }
}
