package com.olchovy.bloomfilter

package object visualizer {

  private val DefaultBitsPerRow = 50

  private val EmptyBit = "\u2610"

  private val FullBit = "\u2612"

  private sealed trait Token

  private case object EmptyToken extends Token {
    override def toString = EmptyBit
  }

  private trait FullToken extends Token {
    override def toString = FullBit
  }

  private case object FilterToken extends FullToken {
    override def toString = Console.CYAN + super.toString + Console.RESET
  }

  private case object SpeculativeToken extends FullToken {
    override def toString = Console.RED + super.toString + Console.RESET
  }

  private case object SharedToken extends FullToken {
    override def toString = Console.MAGENTA + super.toString + Console.RESET
  }

  def printFilter[A](filter: FiniteBloomFilter[A], width: Int = DefaultBitsPerRow) {
    printBitMap(createBitMap(filter), width)
  }

  def printSpeculativeFilter[A](filter: FiniteBloomFilter[A], width: Int = DefaultBitsPerRow)(a: A) {
    printBitMap(createSpeculativeBitMap(filter)(a), width)
  }

  private def createBitMap[A](filter: FiniteBloomFilter[A]): Map[Int, Token] = {
    val bitCount = filter.numberOfSlices * filter.bitsPerSlice
    val bitMapEntries = for(i <- 0 until bitCount) yield {
      if (filter.filter.contains(i))
        (i, FilterToken)
      else
        (i, EmptyToken)
    }

    Map(bitMapEntries: _*)
  }

  private def createSpeculativeBitMap[A](filter: FiniteBloomFilter[A])(a: A): Map[Int, Token] = {
    val bitmap = createBitMap(filter)
    val newBits = filter.bits(filter.keygen(a))

    bitmap.map {
      case (i, EmptyToken) if newBits.contains(i) => (i, SpeculativeToken)
      case (i, _: FullToken) if newBits.contains(i) => (i, SharedToken)
      case mapping @ (i, _) => mapping
    }
  }

  private def drawBitMap(bitmap: Map[Int, Token], width: Int): String = {
    bitmap.toSeq.grouped(width).map(_.map(_._2.toString).mkString(" ")).mkString("\n")
  }

  private def printBitMap(bitmap: Map[Int, Token], width: Int) {
    println(drawBitMap(bitmap, width))
  }

  import collection.mutable.LinkedHashSet

  case class TimeSeries(
      private val filter: FiniteBloomFilter[Int],
      private val visualizationWidth: Int = DefaultBitsPerRow,
      private val _insertionSet: Option[LinkedHashSet[Int]] = None) {

    val insertionSet = _insertionSet.getOrElse(TimeSeries.randomSet(filter.capacity))

    private val entries: Seq[TimeSeries.Entry] = {
      insertionSet.toSeq.map { n =>
        // create speculative visualization to show difference of latest addition
        val visualization = drawBitMap(createSpeculativeBitMap(filter)(n), visualizationWidth)

        // add latest addition to the filter
        filter.add(n)

        // output a new entry
        TimeSeries.Entry(filter.size, n, visualization)
      }
    }

    def save(dirName: String = "/tmp") {
      import java.io.{File, FileOutputStream, ObjectOutputStream}
      import java.util.zip.GZIPOutputStream

      val fileName = dirName + "/series_" + System.nanoTime.toString + ".bin.gz"
      val file = new File(fileName)
      val oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(file)))
      
      try {
        entries.foreach(oos.writeObject(_))
        println("TimeSeries saved to %s".format(fileName))
      } finally {
        oos.close()
      }
    }

    def saveReel(dirName: String = "/tmp", fileName: String = null) {
      import java.io.{File, FileWriter}

      val finalFileName = Option(fileName).map(dirName + "/" + _).getOrElse(dirName + "/series_" + System.nanoTime + ".txt")
      val file = new File(finalFileName)
      val fileWriter = new FileWriter(file)
      
      try {
        val txt = entries.map {
          case TimeSeries.Entry(size, n, visualization) =>
            visualization + "\n" + "adding %d\n".format(n) + "size/capacity: %d/%d\n".format(size, filter.capacity) + "*\n"
        }

        fileWriter.write(txt.mkString)

        println("TimeSeries reel saved to %s".format(finalFileName))
      } finally {
        fileWriter.close()
      }
    }

    def visualize(pauseMillis: Long = 1000L) {
      import java.util.Date

      var continue = true

      entries.foreach {
        case TimeSeries.Entry(size, n, visualization) => try {
          if (continue) {
            println("Size/Capacity: %d/%d".format(size, filter.capacity))
            Thread.sleep(pauseMillis)

            println("Adding %d".format(n))
            Thread.sleep(pauseMillis)

            println(visualization)
            Thread.sleep(pauseMillis)
          }
        } catch {
          case (e: InterruptedException) => continue = false
        }
      }
    }
 
    override def toString = "TimeSeries of %s inserting elements %s".format(filter, insertionSet.mkString(", "))
  }

  object TimeSeries {
    import util.Random

    private case class Entry(n: Int, size: Int, visualization: String)

    @annotation.tailrec
    def randomSet(
        size: Int,
        lowerBound: Int = 0,
        upperBound: Int = Integer.MAX_VALUE,
        acc: LinkedHashSet[Int] = LinkedHashSet.empty): LinkedHashSet[Int] = {
      if (acc.size == size)
        acc
      else
        randomSet(size, lowerBound, upperBound, acc + (Random.nextInt(upperBound - lowerBound) + lowerBound))
    }
  }
}
