package com.olchovy.bloomfilter

import org.scalatest.FunSuite
import collection.mutable.ListBuffer

class BloomFilterSuite extends FunSuite with BloomFilterBehaviors
{
  trait Fixture[A]
  {
    val filter: BloomFilter[A]
  }

  class FiniteFixture[A](capacity: Int, fpp: Double) extends Fixture[A]
  {
    val filter = BloomFilter[A](capacity, fpp)
  }

  class InfiniteFixture[A](fpp: Double) extends Fixture[A]
  {
    val filter = BloomFilter[A](-1, fpp)
  }

  def containsStringTest = (filter: BloomFilter[String]) => {
    filter.add("apple")
    assert(filter.contains("apple"))

    filter.add("banana")
    assert(filter.contains("banana"))

    filter.add("cucumber")
    assert(filter.contains("cucumber"))
  }

  def containsDoubleTest = (filter: BloomFilter[Double]) => {
    filter.add(11d)
    assert(filter.contains(11.0))

    filter.add(3.14)
    assert(filter.contains(3.14))

    filter.add(42.0)
    assert(filter.contains(42.0))
  }

  def capacityTest = (filter: BloomFilter[Int]) => {
    for(i <- 0 until capacity) filter.add(i)
    assert(filter.contains(0))
    assert(filter.contains(capacity - 1))
    assert(filter.contains(capacity / 2))
    assert(fpr(filter, capacity) <= filter.fpp)
  }

  fpps.foreach { fpp =>
    test("finite filter(%d, %g) contains".format(capacity, fpp)) {
      new FiniteFixture[String](capacity, fpp) {
        containsStringTest(filter)
      }

      new FiniteFixture[Double](capacity, fpp) {
        containsDoubleTest(filter)
      }
    }

    test("infinite filter(%g) contains".format(fpp)) {
      new InfiniteFixture[String](fpp) {
        containsStringTest(filter)
      }

      new InfiniteFixture[Double](fpp) {
        containsDoubleTest(filter)
      }
    }
  }

  fpps.foreach { fpp =>
    test("finite filter(%d, %g) capacity".format(capacity, fpp)) {
      new FiniteFixture[Int](capacity, fpp) {
        capacityTest(filter)
      }
    }

    test("infinite filter(%g) capacity".format(fpp)) {
      new InfiniteFixture[Int](fpp) {
        capacityTest(filter)
      }
    }
  }
}

trait BloomFilterBehaviors
{
  this: FunSuite =>

  val capacity = 1000

  val fpps = Seq(0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.01, 0.001, 0.0001, 0.00001)

  def fpr[A](filter: BloomFilter[A], n: Int): Double = {
    math.abs((filter.size.toDouble / n) - 1)
  }
}

/*
object BloomFilterTest
{
  val fpps = Seq(0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.01, 0.001, 0.0001, 0.00001)

  def finiteBloomFilterTest {
    val n = 1000

    fpps.foreach { fpp =>
      val bf = InfiniteBloomFilter[Int](n, fpp)
      for(i <- 0 until n) bf.add(i)

      println(bf)
      assert(bf.contains(0))
      assert(bf.size <= bf.capacity)
      assert(fpr(bf, n) <= bf.fpp)
    }
  }

  def infiniteBloomFilterTest {
    val n = 1000
    val nnn = n * 3

    fpps.foreach { fpp =>
      val bf = InfiniteBloomFilter[Int](n, fpp)
      for(i <- 0 until nnn) bf.add(i)

      println(bf)
      assert(fpr(bf, nnn) <= bf.fpp)
    }
  }

  private def fpr[A](bf: BloomFilter[A], n: Int): Double = {
    math.abs((bf.size.toDouble / n) - 1)
  }
}
*/
