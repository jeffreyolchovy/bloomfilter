package com.olchovy.bloomfilter

import java.nio.ByteBuffer
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

  def serialize: Array[Byte]

  override def toString: String = "%s [cap=%s fpp=%s size=%s]".format(this.getClass.getName, capacity, fpp, size)

  if(fpp <= 0 || fpp >= 1) throw new IllegalArgumentException("[fpp] must be on the interval (0,1)")
}

object BloomFilter
{
  private val DefaultCapacity: Int = 10000

  private val DefaultFPP: Double = 0.01

  def murmurHash(value: String, seed: Int): Int = {
    val f = new MurmurHash(seed)
    value.getBytes.map(_.toInt).foreach(f.append _)
    f.hash
  }

  def bernsteinHash(value: String, seed: Int): Int = {
    value.foldLeft(5381 ^ seed)((acc, char) => (acc << 5) + acc + char) & 0xF7777777
  }

  def apply[A](capacity: Int, fpp: Double): BloomFilter[A] = {
    if(capacity == -1)
      InfiniteBloomFilter[A](DefaultCapacity, fpp)
    else if(capacity > 0)
      FiniteBloomFilter(capacity, fpp)
    else
      throw new IllegalArgumentException("[capacity] must be -1 or a positive value")
  }

  def apply[A](capacity: Int): BloomFilter[A] = apply(capacity, DefaultFPP)

  def deserialize[A](bytes: Array[Byte]): BloomFilter[A] = readFrom[A](ByteBuffer.wrap(bytes))

  private[bloomfilter] def readFrom[A](buffer: ByteBuffer): BloomFilter[A] = {
    if(buffer.getInt(0) == -1) InfiniteBloomFilter.readFrom[A](buffer) else FiniteBloomFilter.readFrom[A](buffer)
  }
}
