package com.olchovy.bloomfilter

import scala.collection.mutable.{BitSet, ListBuffer}
import scala.util.MurmurHash

trait BloomFilter[A]
{
  /** max n elements without exceeding fpp */
  val capacity: Int

  /** false positive probability */
  val fpp: Double

  def size: Int

  def isFull: Boolean

  def contains(a: A): Boolean 

  def add(a: A): Boolean
  
  override def toString: String = "%s [cap=%s fpp=%s size=%s]".format(this.getClass.getName, capacity, fpp, size)

  if(fpp <= 0 || fpp >= 1) throw new IllegalArgumentException("[fpp] must be on the interval (0,1)")
}

case class FiniteBloomFilter[A](capacity: Int, fpp: Double) extends BloomFilter[A]
{
  import BloomFilter._

  protected val numberOfSlices: Int = {
    math.ceil(math.log(1 / fpp) / math.log(2)).toInt
  }

  protected val bitsPerSlice: Int = {
    math.ceil((2 * capacity * math.abs(math.log(fpp))) / (numberOfSlices * math.pow(math.log(2), 2))).toInt
  }

  protected val filter = new BitSet(numberOfSlices * bitsPerSlice)

  protected var count = 0

  def size = count

  def isFull = count > capacity

  def contains(a: A): Boolean = {
    val key = a.hashCode.toString
    bits(key).subsetOf(filter)
  }

  def add(a: A): Boolean = {
    if(contains(a)) true else add_!(a)
  }

  protected def add_!(a: A): Boolean = {
    if(isFull) false else {
      val key = a.hashCode.toString
      filter ++= bits(key)
      count += 1
      true
    }
  }

  protected def bits(key: String): BitSet = {
    val bs = new BitSet(numberOfSlices * bitsPerSlice)
    val x = hash(key, 0)
    val y = hash(key, x)
    var offset = 0

    for(i <- 0 until numberOfSlices) {
      bs(math.abs((x + i * y) % bitsPerSlice) + offset) = true
      offset += bitsPerSlice
    }

    bs
  }

  override def toString: String = "%s [%d x %d]".format(super.toString, numberOfSlices, bitsPerSlice)

  if(capacity <= 0) throw new IllegalArgumentException("[capacity] must be a positive value")
}

case class InfiniteBloomFilter[A](initialCapacity: Int = 1000, fpp: Double) extends BloomFilter[A]
{
  val capacity = -1

  val filters = ListBuffer[FiniteBloomFilter[A]](new FiniteBloomFilter[A](initialCapacity, fpp))

  def size = filters.foldLeft(0) { (acc, filter) => acc + filter.size }

  def isFull = false

  def contains(a: A): Boolean = {
    filters.reverse.find(_.contains(a)).isDefined
  }

  def add(a: A): Boolean = {
    if(filters.head.isFull)
      filters.prepend(FiniteBloomFilter[A](initialCapacity, fpp))

    filters.head.add(a)
  }
}

object BloomFilter
{
  private val DEFAULT_CAPACITY: Int = 10000

  private val DEFAULT_FPP: Double = 0.01

  private[bloomfilter] def hash(value: String, seed: Int): Int = {
    val f = new MurmurHash(seed)
    value.getBytes.map(_.toInt).foreach(f.append _)
    f.hash
  }

  def apply[A](capacity: Int, fpp: Double): BloomFilter[A] = {
    if(capacity == 0 || capacity == -1) 
      InfiniteBloomFilter[A](DEFAULT_CAPACITY, fpp)
    else if(capacity > 0)
      FiniteBloomFilter(capacity, fpp)
    else
      throw new IllegalArgumentException("[capacity] must be -1, 0, or a positive value")
  }

  def apply[A](capacity: Int): BloomFilter[A] = apply(capacity, DEFAULT_FPP)
}
