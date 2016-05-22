package com.olchovy.bloomfilter

import java.io.File
import scala.io.Source
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.slf4j.LoggerFactory

class BloomFilterSuite extends FunSuite with BloomFilterBehaviors {

  private val log = LoggerFactory.getLogger(getClass)

  trait Fixture[A] {
    val filter: BloomFilter[A]
  }

  class FiniteFixture[A : Hashable](capacity: Int, fpp: Double) extends Fixture[A] {
    val filter = BloomFilter[A](capacity, fpp)
  }

  class InfiniteFixture[A : Hashable](fpp: Double) extends Fixture[A] {
    val filter = BloomFilter[A](-1, fpp)
  }

  def mightContainStringTest = (filter: BloomFilter[String]) => {
    filter.put("apple")
    assert(filter.mightContain("apple"))
    filter.put("banana")
    assert(filter.mightContain("banana"))
    filter.put("coriander")
    assert(filter.mightContain("coriander"))
    assert(!filter.mightContain("dill"))
    log.debug(s"$filter")
  }

  def mightContainDoubleTest = (filter: BloomFilter[Double]) => {
    filter.put(11d)
    assert(filter.mightContain(11.0))
    filter.put(3.14)
    assert(filter.mightContain(3.14))
    filter.put(42.0)
    assert(filter.mightContain(42.0))
    assert(!filter.mightContain(137d))
    log.debug(s"$filter")
  }

  def capacityTest = (filter: BloomFilter[Int]) => {
    for (i <- 0 until capacity) {
      filter.put(i)
    }
    assert(filter.mightContain(0))
    assert(filter.mightContain(capacity - 1))
    assert(filter.mightContain(capacity / 2))
    assert(fpr(filter, capacity) <= filter.fpp)
    log.debug(s"$filter")
  }

  def fppTest = (filter: BloomFilter[Int]) => {
    for (i <- 0 until capacity) {
      assert(!filter.mightContain(i))
      filter.put(i)
    }
    assert(fpr(filter, capacity) <= filter.fpp)
    log.debug(s"$filter")
  }

  fpps.foreach { fpp =>
    new FiniteFixture[String](3, fpp) {
      test(s"$filter mightContain[String]") {
        mightContainStringTest(filter)
      }
    }
    new FiniteFixture[Double](3, fpp) {
      test(s"$filter mightContain[Double]") {
        mightContainDoubleTest(filter)
      }
    }
    new InfiniteFixture[String](fpp) {
      test(s"$filter mightContain[String]") {
        mightContainStringTest(filter)
      }
    }
    new InfiniteFixture[Double](fpp) {
      test(s"$filter mightContain[Double]") {
        mightContainDoubleTest(filter)
      }
    }
  }

  fpps.foreach { fpp =>
    new FiniteFixture[Int](capacity, fpp) {
      test(s"$filter capacity".format(capacity, fpp)) {
        capacityTest(filter)
      }
    }
    new InfiniteFixture[Int](fpp) {
      test(s"$filter capacity".format(fpp)) {
        capacityTest(filter)
      }
    }
  }

  fpps.filter(_ <= 0.01).foreach { fpp =>
    new FiniteFixture[Int](capacity, fpp) {
      test(s"$filter fpp".format(capacity, fpp)) {
        fppTest(filter)
      }
    }

    new InfiniteFixture[Int](fpp) {
      test(s"$filter fpp".format(fpp)) {
        fppTest(filter)
      }
    }
  }

  test("insertion counts with infinite filters") {
    val filter = InfiniteBloomFilter[String](2, 0.000001, 0.5)
    val insertions = 10
    for (i <- 1 to insertions) filter.put(i.toString)
    assert(filter.insertions == insertions)
  }

  test(s"insert counting numbers") {
    val insertions = 10 * 1000
    val fpp = 1D / (10 * 1000)
    new InfiniteFixture[Int](fpp) {
      val startTime = System.currentTimeMillis
      var falsePositivesCount = 0
      var falsePositives = Set.newBuilder[Int]
      for (i <- 1 to insertions) {
        if (filter.mightContain(i)) {
          falsePositivesCount += 1
          falsePositives += i
        }
        filter.put(i)
      }
      val elapsedTime = System.currentTimeMillis - startTime
      log.info("Time elapsed: %,d milliseconds".format(elapsedTime))
      log.info(s"$filter had $falsePositivesCount/$insertions false positives: ${falsePositives.result}")
    }
  }

  test("insert /usr/share/dict/words") {
    val file = new File("/usr/share/dict/words")
    if (file.exists) {
      val insertions = Seq(10, 50, 100).map(_ * 1000)
      val fpp = 1D / (10 * 1000)
      val fixtures = Set(
        (i: Int) => new FiniteFixture[String](i, fpp),
        (_: Int) => new InfiniteFixture[String](fpp)
      )
      for {
        i <- insertions
        f <- fixtures
        fixture = f(i)
        words = Source.fromFile(file).getLines.take(i)
      } {
        import fixture._
        val startTime = System.currentTimeMillis
        var wordCount = 0
        var falsePositivesCount = 0
        var falsePositives = Set.newBuilder[String]
        for (word <- words) {
          if (filter.mightContain(word)) {
            falsePositivesCount += 1
            falsePositives += word
          }
          filter.put(word)
          wordCount += 1
        }
        val elapsedTime = System.currentTimeMillis - startTime
        log.info("Time elapsed: %,d milliseconds".format(elapsedTime))
        log.info(s"$filter had $falsePositivesCount/$wordCount false positives: ${falsePositives.result}")
      }
    }
  }
}

trait BloomFilterBehaviors {

  this: FunSuite =>

  val capacity = 10 * 1000

  val fpps = Seq(0.1, 0.01, 0.001, 0.0001, 0.00001)

  def fpr[A](filter: BloomFilter[A], n: Int): Double = {
    math.abs((filter.insertions.toDouble / n) - 1)
  }
}
