/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Ángel Cervera Claudio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.acervera.osm4scala

import java.io._

import org.openstreetmap.osmosis.osmbinary.fileformat.{Blob, BlobHeader}

object BlobTupleIterator {

  /**
    * Create a new BlobTupleIterator iterator from a IntputStream pbf format.
    *
    * @param pbfInputStream Opened InputStream that contains the pbf
    * @return
    */
  def fromPbf(pbfInputStream: InputStream) = new BlobTupleIterator(pbfInputStream)

}

/**
  * Iterator over a OSM file in pbf format.
  * Each item is a tuple of BlobHeader and Blob
  *
  * Assume that the pbf file is a secuence of three element in this order:
  * 1. Size of the next BlobHeader
  * 2. BlobHeader, with the size of the next blob.
  * 3. Blob.
  *
  * @param pbfInputStream Input stream that will be used to read the fileblock
  * @author angelcervera
  */
class BlobTupleIterator(pbfInputStream: InputStream) extends Iterator[(BlobHeader, Blob)] {

  // Read the input stream using DataInputStream to access easily to Int and raw fields.
  val pbfStream = new DataInputStream(pbfInputStream)

  // Store the next block length. None if there are not more to read.
  var nextBlockLength: Option[Int] = None

  // Read the length of the next block
  readNextBlockLength

  override def hasNext: Boolean = nextBlockLength.isDefined

  /**
    * Takes the next "length" bytes from the stream and parsers it as a BlobHeader
    *
    * @param length size of the next BlobHeader
    * @param is Stream with the data
    * @return New BlobHeader
    */
  private def readNextHeader(length: Int, is: DataInputStream) : BlobHeader = {
    // Fill the buffer with data from "is".
    val bufferBlobHeader = new Array[Byte](length)
    pbfStream.readFully(bufferBlobHeader)

    // Parsing header.
    BlobHeader parseFrom bufferBlobHeader
  }

  /**
    * Takes the next "length" bytes from the stream and parsers it as a Blob
    *
    * @param length size of the next Blob
    * @param is Stream with the data
    * @return New Blob
    */
  private def readNextData(length: Int, is: DataInputStream) : Blob = {
    // Fill the buffer with data from "is".
    val bufferBlob = new Array[Byte](length)
    pbfStream.readFully(bufferBlob)

    // Parsing the blob.
    Blob parseFrom bufferBlob
  }

  override def next(): (BlobHeader, Blob) = {

    val blobHeader = readNextHeader(nextBlockLength.get, pbfStream)

    // Parsing the blob
    val blob = readNextData(blobHeader.datasize, pbfStream)

    // Move to the next pair.
    readNextBlockLength

    (blobHeader, blob)

  }

  /**
    * Read the next osm pbf block
    */
  private def readNextBlockLength() = {

    try {
      nextBlockLength = Some(pbfStream.readInt)
    } catch {
      case _: EOFException => nextBlockLength = None
    }

  }

}
