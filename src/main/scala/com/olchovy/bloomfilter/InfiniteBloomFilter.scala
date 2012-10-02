package com.olchovy.bloomfilter

import java.nio.ByteBuffer
import scala.collection.mutable.ListBuffer

/* A bloom filter that has no ceiling on its capacity */
case class InfiniteBloomFilter[A](initialCapacity: Int = 1000, fpp: Double) extends BloomFilter[A]
{
  val capacity = -1

  protected val filters = ListBuffer[FiniteBloomFilter[A]](new FiniteBloomFilter[A](initialCapacity, fpp))

  def size = filters.foldLeft(0) { (acc, filter) => acc + filter.size }

  def isFull = false

  def contains(a: A): Boolean = {
    filters.reverse.find(_.contains(a)).isDefined
  }

  def add(a: A): Boolean = {
    if(filters.head.isFull) filters.prepend(FiniteBloomFilter[A](initialCapacity, fpp))
    filters.head.add(a)
  }

  def serialize: Array[Byte] = throw new UnsupportedOperationException
}

object InfiniteBloomFilter
{
  def deserialize[A](bytes: Array[Byte]): InfiniteBloomFilter[A] = readFrom[A](ByteBuffer.wrap(bytes))

  /* start reading from offset 4, since first 4 bytes are sentinel value marking it as an infinite filter */
  private[bloomfilter] def readFrom[A](buffer: ByteBuffer): InfiniteBloomFilter[A] = {
    throw new UnsupportedOperationException
  }
}
