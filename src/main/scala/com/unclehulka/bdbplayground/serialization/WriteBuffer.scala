package com.unclehulka.bdbplayground.serialization

import java.lang.System.{arraycopy => copy}

class WriteBuffer(initialCapacity: Int) {
  private[this] var buf = BufferRecycler.allocBuffer(initialCapacity)
  private[this] var count = 0

  /**
   * Writes a single byte to the buffer.
   */
  def writeByte(v: Byte) {
    val newCount = count + 1
    ensureCapacity(newCount)
    buf(count) = v
    count = newCount
  }

  /**
   * Writes a short as a 16-bit, fixed-length, big-endian value.
   */
  def writeShort(v: Short) {
    val newCount = count + 2
    ensureCapacity(newCount)
    buf(count)     = (v >>> 8).toByte
    buf(count + 1) = v.toByte
    count = newCount
  }

  /**
   * Writes an int as a 32-bit, fixed-length, big-endian value.
   */
  def writeInt(v: Int) {
    val newCount = count + 4
    ensureCapacity(newCount)
    buf(count) =     (v >>> 24).toByte
    buf(count + 1) = (v >>> 16).toByte
    buf(count + 2) = (v >>>  8).toByte
    buf(count + 3) = v.toByte
    count = newCount
  }

  /**
   * Writes a long as a 64-bit, fixed-length, big-endian value.
   */
  def writeLong(v: Long) {
    val newCount = count + 8
    ensureCapacity(newCount)
    buf(count) =     (v >>> 56).toByte
    buf(count + 1) = (v >>> 48).toByte
    buf(count + 2) = (v >>> 40).toByte
    buf(count + 3) = (v >>> 32).toByte
    buf(count + 4) = (v >>> 24).toByte
    buf(count + 5) = (v >>> 16).toByte
    buf(count + 6) = (v >>> 8).toByte
    buf(count + 7) = v.toByte
    count = newCount
  }

  /**
   * Writes an int as a variable-length value.
   */
  def writeVarInt(v: Int) {
    ensureCapacity(count + 5)
    var n = v
    n = (n << 1) ^ (n >> 31) // move sign to low-order bit
    while ((n & ~0x7f) != 0) {
      buf(count) = ((n & 0x7f) | 0x80).toByte
      count += 1
      n >>>= 7
    }
    buf(count) = n.toByte
    count += 1
  }

  /**
   * Writes a long as a variable-length value.
   */
  def writeVarLong(v: Long) {
    ensureCapacity(count + 9)
    var n = v
    n = (n << 1) ^ (n >> 63) // move sign to low-order bit
    while ((n & ~0x7f) != 0) {
      buf(count) = ((n & 0x7f) | 0x80).toByte
      count += 1
      n >>>= 7
    }
    buf(count) = n.toByte
    count += 1
  }

  /**
   * Writes the contents of the given byte array.
   */
  def writeBytes(b: Array[Byte]) {
    writeBytes(b, 0, b.length)
  }

  /**
   * Writes the slice of the given byte array.
   */
  def writeBytes(b: Array[Byte], offset: Int, length: Int) {
    val newCount = count + length
    ensureCapacity(newCount)
    copy(b, offset, buf, count, length)
    count = newCount
  }

  /**
   * Write a string as a UTF8-encoded byte sequence.
   */
  def writeUtf8String(s: String) {
    val bytes = s.getBytes(Charset.UTF8)
    writeVarInt(bytes.length)
    writeBytes(bytes)
  }

  /**
   * Releases any intermediate buffers and returns the final byte array.
   */
  def finish() = {
    val bytes = new Array[Byte](count)
    copy(buf, 0, bytes, 0, count)
    BufferRecycler.releaseBuffer(buf)
    bytes
  }

  private def ensureCapacity(requiredSize: Int) = {
    if (buf.length < requiredSize) {
      val newBuffer = BufferRecycler.allocBuffer(math.max(buf.length << 1, requiredSize))
      copy(buf, 0, newBuffer, 0, count)
      BufferRecycler.releaseBuffer(buf)
      buf = newBuffer
    }
    buf
  }
}
