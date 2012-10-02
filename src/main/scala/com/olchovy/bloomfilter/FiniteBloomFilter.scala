package com.olchovy.bloomfilter

import java.math.BigInteger
import java.nio.ByteBuffer
import scala.collection.mutable.BitSet

/* A concrete implementation of a Bloom filter that guarantees a false positive probability
 * up until its capacity is exceeded.
 *  
 * For more information on partitioning the total number of m bits among the k hash functions,
 * and the necessary, subsequent creation of the k slices of m/k bits:
 *
 * @see http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf 
 *
 * And, although less helpful, a general overview of the calculation for the total number of bits (from Wikipedia):
 *
 * The required number of bits m, given n capacity and a desired false positive probability p
 * (and assuming the optimal value of k hashes is used) can be computed by [...]:
 *
 *    m = -((n ln p)/((ln 2)^2)) 
 *
 * @see http://en.wikipedia.org/w/index.php?title=Bloom_filter#Probability_of_false_positives
 */
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

  def contains(a: A): Boolean = contains(bits(keygen(a)))

  /* this will delegate to `contains(b: BitSet)` and `add(b: BitSet)` to avoid recomputing the hash */
  def add(a: A): Boolean = {
    val bitset = bits(keygen(a))
    if(contains(bitset)) true else add(bitset)
  }

  def serialize: Array[Byte] = {
    val length = if(filter.isEmpty) 0 else filter.max + 1
    val hexString = if(length == 0) "" else {
      val bits = Array.fill(length)(0)
      for(n <- filter) bits(n) = 1
      new BigInteger(bits.mkString, 2).toString(16)
    }
    val hexBytes = hexString.getBytes
    val bufferSize = 4 + 8 + 4 + 4 + hexBytes.size
    val buffer = ByteBuffer.allocate(bufferSize)
    buffer.putInt(capacity)
    buffer.putDouble(fpp)
    buffer.putInt(count)
    buffer.putInt(length)
    //for(byte <- hexBytes) buffer.put(byte)
    buffer.put(hexBytes, 0, hexBytes.size)
    buffer.array
  }

  protected def keygen(a: A): String = a.hashCode.toString

  private def bits(key: String): BitSet = {
    val bitset = new BitSet(numberOfSlices * bitsPerSlice)
    val x = hash(key, 0)
    val y = hash(key, x)
    var offset = 0

    for(i <- 0 until numberOfSlices) {
      bitset(math.abs((x + i * y) % bitsPerSlice) + offset) = true
      offset += bitsPerSlice
    }

    bitset
  }

  private def contains(bitset: BitSet): Boolean = bitset.subsetOf(filter)

  private def add(bitset: BitSet): Boolean = {
    if(isFull) false else {
      filter ++= bitset
      count += 1
      true
    }
  }

  override def toString: String = "%s [%d x %d]".format(super.toString, numberOfSlices, bitsPerSlice)

  if(capacity <= 0) throw new IllegalArgumentException("[capacity] must be a positive value")
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
      for((n, i) <- binaryString.zipWithIndex if n == '1') bitset(i) = true
    }

    new FiniteBloomFilter[A](capacity, fpp) {
      override protected val filter = bitset
      count = previousCount
    }
  }
}
