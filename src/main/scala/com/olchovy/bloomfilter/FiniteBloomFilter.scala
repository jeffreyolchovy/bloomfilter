package com.olchovy.bloomfilter

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.BitSet

/* A concrete implementation of a Bloom filter that guarantees a false positive probability
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
case class FiniteBloomFilter[A](capacity: Int, fpp: Double) extends BloomFilter[A]
{
  import BloomFilter._

  /** The probability of a given bit being set in a slice
   *
   * For any given error probability P and filter size M,
   * n is maximized by making p = 1/2, regardless of P or M.
   *
   * As p corresponds to the fill ratio of a slice,
   * a filter depicts an optimal use when slices are half full.
   */
  protected val p = 0.5

  // number of slices
  protected val k: Int = {
    math.ceil(math.log(1 / fpp) / math.log(2)).toInt
  }

  // bits per slice
  protected val m: Int = {
    math.ceil(((1 / p) * capacity * math.abs(math.log(fpp))) / (k * math.pow(math.log(2), 2))).toInt
  }

  // total bits
  protected val M: Int = k * m

  protected val filter = new BitSet(M)

  protected var count = 0

  def size = count

  def isFull = count >= capacity

  def contains(a: A): Boolean = contains(bits(keygen(a)))

  /* this will delegate to `contains(b: BitSet)` and `add(b: BitSet)` to avoid recomputing the hash */
  def add(a: A): Boolean = {
    val bitset = bits(keygen(a))
    if(contains(bitset)) true else add(bitset)
  }

  def serialize: Array[Byte] = {
    val length = if(filter.isEmpty) 0 else filter.length + 1
    val hexString = if(length == 0) "" else {
      val bits = Array.fill(length)(0)
      for(n <- getSetBits()) bits(n) = 1
      new BigInteger(bits.mkString, 2).toString(16)
    }
    val hexBytes = hexString.getBytes
    val bufferSize = 4 + 8 + 4 + 4 + hexBytes.size
    val buffer = ByteBuffer.allocate(bufferSize)
    buffer.putInt(capacity)
    buffer.putDouble(fpp)
    buffer.putInt(count)
    buffer.putInt(length)
    buffer.put(hexBytes, 0, hexBytes.size)
    buffer.array
  }
  
  protected def keygen(a: A): String = a.hashCode.toString

  private def bits(key: String): BitSet = {
    val bitset = new BitSet
    val x = hash(key, 0)
    val y = hash(key, x)
    var offset = 0

    for(i <- 0 until k) {
      bitset.set(math.abs((x + i * y) % m) + offset)
      offset += m
    }

    bitset
  }

  private def contains(bitset: BitSet): Boolean = {
    val bitset_ = bitset.clone().asInstanceOf[BitSet]
    bitset_.and(filter)
    bitset.cardinality == bitset_.cardinality
  }

  private def add(bitset: BitSet): Boolean = {
    if(isFull) false else {
      filter.or(bitset)
      count += 1
      true
    }
  }

  @annotation.tailrec
  private def getSetBits(from: Int = 0, setBits: List[Int] = Nil): List[Int] = {
    filter.nextSetBit(from) match {
      case -1 if setBits.nonEmpty => setBits.reverse
      case -1 => setBits
      case i => getSetBits(i + 1, i :: setBits)
    } 
  }

  override def toString: String = "%s [%d x %d]".format(super.toString, k, m)

  if(capacity <= 0) throw new IllegalArgumentException("[capacity] must be a positive value")

  if(M > Integer.MAX_VALUE) throw new IllegalArgumentException("Total number of bits exceeds maximum")
}

object FiniteBloomFilter
{
  def deserialize[A](bytes: Array[Byte]): FiniteBloomFilter[A] = readFrom[A](ByteBuffer.wrap(bytes))

  private[bloomfilter] def readFrom[A](buffer: ByteBuffer): FiniteBloomFilter[A] = {
    val capacity = buffer.getInt(0)
    val fpp = buffer.getDouble(4)
    val previousCount = buffer.getInt(12)
    val length = buffer.getInt(16)
    val hexString = new String(buffer.array.slice(20, buffer.capacity))
    val bitset = new BitSet(length)

    if(!hexString.isEmpty) {
      val binaryString = new BigInteger(hexString, 16).toString(2).reverse.padTo(length, "0").reverse
      for((n, i) <- binaryString.zipWithIndex if n == '1') bitset.set(i)
    }

    new FiniteBloomFilter[A](capacity, fpp) {
      override protected val filter = bitset
      count = previousCount
    }
  }
}
