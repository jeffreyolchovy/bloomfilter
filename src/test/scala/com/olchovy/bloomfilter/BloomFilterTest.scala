package com.olchovy.bloomfilter

import java.util.Date
import scala.io.Source
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import collection.mutable.ListBuffer

class BloomFilterSuite extends FunSuite with ShouldMatchers with BloomFilterBehaviors
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

    filter.add("coriander")
    assert(filter.contains("coriander"))

    assert(!filter.contains("dill"))
  }

  def containsDoubleTest = (filter: BloomFilter[Double]) => {
    filter.add(11d)
    assert(filter.contains(11.0))

    filter.add(3.14)
    assert(filter.contains(3.14))

    filter.add(42.0)
    assert(filter.contains(42.0))

    assert(!filter.contains(137d))
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

  test("insert /usr/share/dict/words") {
    val words = Source.fromFile("/usr/share/dict/words").getLines.take(100 * 1000)

    new InfiniteFixture[String](0.1) {
      val startTime = (new Date).getTime

      for(word <- words) {
        filter.add(word)
      }

      val endTime = (new Date).getTime
      val elapsedTime = endTime - startTime

      println("Time elapsed: %d".format(elapsedTime))
    }
  }

  test("serialization and deserialization") {
    new FiniteFixture[Int](10, 0.01) {
      filter.add(1)
      filter.add(3)
      filter.add(5)

      assert(filter.contains(1))
      assert(!filter.contains(2))
      assert(filter.contains(3))
      assert(!filter.contains(4))
      assert(filter.contains(5))

      val serialized = filter.serialize
      val deserialized = BloomFilter.deserialize[Int](serialized)

      assert(filter.capacity == deserialized.capacity)
      assert(filter.fpp == deserialized.fpp)
      assert(filter.size == deserialized.size)

      assert(deserialized.contains(1))
      assert(!deserialized.contains(2))
      assert(deserialized.contains(3))
      assert(!deserialized.contains(4))
      assert(deserialized.contains(5))
    }
  }

  test("serialization and deserialization of empty filter") {
    new FiniteFixture[String](1000, 0.005) {
      val serialized = filter.serialize
      val deserialized = BloomFilter.deserialize[Int](serialized)
      assert(filter.capacity == deserialized.capacity)
      assert(filter.fpp == deserialized.fpp)
      assert(filter.size == deserialized.size)
    }
  }

  test("serialize and convert <-> hex string") {
    new FiniteFixture[String](10, 0.01) {
      filter.add("abc")
      filter.add("def")
      val serialized = filter.serialize
      val hexString = serialized.map("%02x".format(_)).mkString
      val bytes = hexString.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      serialized should equal (bytes)
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

