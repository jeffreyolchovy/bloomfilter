package com.olchovy.bloomfilter

trait BloomFilter[A] {

  /** Max number of elements this can contain without exceeding the fpp */
  val capacity: Int

  /** False positive probability */
  val fpp: Double

  if (fpp <= 0 || fpp >= 1) throw new IllegalArgumentException("[fpp] must be on the interval (0,1)")

  def insertions: Long

  def put(a: A): Unit

  def mightContain(a: A): Boolean

  override def toString: String = "%s [insertions=%d/%d fpp=%g]".format(
    this.getClass.getSimpleName,
    insertions,
    capacity,
    fpp
  )
}

object BloomFilter {

  val DefaultCapacity: Int = 10000

  val DefaultFpp: Double = 0.01

  val DefaultGrowthRate: Double = 0.9

  def apply[A : Hashable](capacity: Int = DefaultCapacity, fpp: Double = DefaultFpp): BloomFilter[A] = {
    if (capacity == -1) {
      InfiniteBloomFilter[A](DefaultCapacity, fpp, DefaultGrowthRate)
    } else if (capacity > 0) {
      FiniteBloomFilter(capacity, fpp)
    } else {
      throw new IllegalArgumentException("[capacity] must be -1 or a positive value")
    }
  }
}
