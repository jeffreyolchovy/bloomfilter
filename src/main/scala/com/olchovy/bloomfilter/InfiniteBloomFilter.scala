package com.olchovy.bloomfilter

import scala.collection.mutable.ListBuffer

/** A Bloom filter that has no ceiling on its capacity */
case class InfiniteBloomFilter[A : Hashable](initialCapacity: Int, fpp: Double, growthRate: Double) extends BloomFilter[A] {

  val capacity = -1

  private val underlying = ListBuffer[FiniteBloomFilter[A]](new FiniteBloomFilter[A](initialCapacity, fpp))

  def insertions: Long = underlying.map(_.insertions).sum

  def put(a: A): Unit = {
    val current = underlying.head
    if (current.insertions < current.capacity) {
      current.put(a)
    } else {
      val next = nextUnderlying()
      underlying.prepend(next)
      next.put(a)
    }
  }

  def mightContain(a: A): Boolean = {
    underlying.foldLeft(false) { (acc, next) =>
      acc || next.mightContain(a)
    }
  }

  private def nextUnderlying(): FiniteBloomFilter[A] = {
    val i = underlying.size
    val nextCapacity = (initialCapacity * math.pow(1 / growthRate, i)).toInt
    val nextFpp = fpp * math.pow(growthRate, i)
    FiniteBloomFilter[A](nextCapacity, nextFpp)
  }

  override def toString: String = "%s [insertions=%d/∞ fpp=%g] [filters=%d bits=%d]".format(
    this.getClass.getSimpleName,
    insertions,
    fpp,
    underlying.size,
    underlying.map(_.M).sum
  )
}
