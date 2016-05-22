package com.olchovy.bloomfilter

import scala.math.BigInt._
import scala.util.hashing.MurmurHash3
import scala.util.Random

trait Hashable[-A] {
  val defaultSeed: Int
  def hash(a: A): Int = hash(a, defaultSeed)
  def hash(a: A, seed: Int): Int
}

object Hashable {

  implicit object HashableString extends Hashable[String] {
    val defaultSeed = MurmurHash3.stringSeed
    def hash(a: String, seed: Int): Int = MurmurHash3.stringHash(a, seed)
  }

  implicit object HashableInt extends Hashable[Int] {
    val defaultSeed = MurmurHash3.arraySeed
    def hash(a: Int, seed: Int): Int = MurmurHash3.bytesHash(a.toByteArray, seed)
  }

  implicit object HashableLong extends Hashable[Long] {
    val defaultSeed = MurmurHash3.arraySeed
    def hash(a: Long, seed: Int): Int = MurmurHash3.bytesHash(a.toByteArray, seed)
  }

  implicit object HashableDouble extends Hashable[Double] {
    val defaultSeed = MurmurHash3.arraySeed
    def hash(a: Double, seed: Int): Int = {
      val longValue = java.lang.Double.doubleToLongBits(a)
      val bytes = Array.fill[Byte](8)(0)
      for (i <- 0 until 8) {
        bytes(i) = ((longValue >> ((7 - i) * 8)) & 0xff).toByte
      }
      MurmurHash3.bytesHash(bytes, seed)
    }
  }

  implicit object HashableBytes extends Hashable[Array[Byte]] {
    val defaultSeed = MurmurHash3.arraySeed
    def hash(a: Array[Byte], seed: Int): Int = MurmurHash3.bytesHash(a, seed)
  }

  implicit object HashableProduct extends Hashable[Product] {
    val defaultSeed = MurmurHash3.productSeed
    def hash(a: Product, seed: Int): Int = MurmurHash3.productHash(a, seed)
  }
}
