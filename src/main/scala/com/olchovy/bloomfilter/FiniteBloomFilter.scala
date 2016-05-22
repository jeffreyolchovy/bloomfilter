package com.olchovy.bloomfilter

import java.util.BitSet
import scala.util.hashing.MurmurHash3

/** A concrete implementation of a Bloom filter that guarantees a false positive probability
  * up until its capacity is exceeded.
  *
  * For more information on partitioning the total number of M bits among the k hash functions,
  * and the necessary, subsequent creation of the k slices of m [M/k] bits:
  *
  * @see http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
  *
  * And, although less helpful, a general overview of the calculation for the total number of bits (from Wikipedia):
  *
  * The required number of bits M, given n capacity and a desired false positive probability P
  * (and assuming the optimal value of k hashes is used) can be computed by [...]:
  *
  *    M = -((n ln P)/((ln 2)^2))
  *
  * @see http://en.wikipedia.org/w/index.php?title=Bloom_filter#Probability_of_false_positives
  */
case class FiniteBloomFilter[A : Hashable](capacity: Int, fpp: Double) extends BloomFilter[A] {

  /** The probability of a given bit being set in a slice
    *
    * For any given error probability P and filter size M,
    * n is maximized by making p = 1/2, regardless of P or M.
    *
    * As p corresponds to the fill ratio of a slice,
    * a filter depicts an optimal use when slices are half full.
   */
  val p = 0.5

  /** Number of slices */
  val k: Int = {
    math.ceil(math.log(1 / fpp) / math.log(2)).toInt
  }

  /** Bits per slice */
  val m: Int = {
    math.ceil(((1 / p) * capacity * math.abs(math.log(fpp))) / (k * math.pow(math.log(2), 2))).toInt
  }

  /** Total bits */
  val M: Int = k * m

  if (capacity <= 0) {
    throw new IllegalArgumentException("[capacity] must be a positive value")
  }

  if (M > Int.MaxValue) {
    throw new IllegalArgumentException("Total number of bits exceeds maximum")
  }

  private val hashable = implicitly[Hashable[A]]

  private val underlyingBitSet = new BitSet(M)

  @volatile
  private var count: Int = 0

  def insertions: Long = count

  def bitSet: BitSet = underlyingBitSet.clone().asInstanceOf[BitSet]

  def put(a: A): Unit = {
    if (count < capacity) {
      val bitSetA = getBitSet(a)
      underlyingBitSet.or(bitSetA)
      count += 1
    }
  }

  def mightContain(a: A): Boolean = {
    val bitSetA = getBitSet(a)
    containsBitSet(bitSetA)
  }

  def containsBitSet(bitSet: BitSet): Boolean = {
    val tmpBitSet = this.bitSet
    tmpBitSet.and(bitSet)
    tmpBitSet.cardinality == bitSet.cardinality
  }

  def getBitSet(a: A): BitSet = {
    val bitSet = new BitSet
    val x = hashable.hash(a)
    val y = hashable.hash(a, x)
    var offset = 0
    for (i <- 0 until k) {
      bitSet.set(math.abs((x + i * y) % m) + offset)
      offset += m
    }
    bitSet
  }

  override def toString: String = "%s [bits=%d]".format(super.toString, M)
}
