package com.unclehulka.bdbplayground.serialization

import java.lang.System.{arraycopy => copy}

/**
 * A read buffer for deserializing values.
 */
class ReadBuffer(private[this] val buf: Array[Byte]) {
  private[this] var pos = 0

  /**
   * Reads the given number of bytes from the buffer returns then as a byte
   * array.
   */
  def readBytes(count: Int): Array[Byte] = {
    val len = math.min(count, buf.length - pos)
    val bytes = new Array[Byte](len)
    copy(buf, pos, bytes, 0, len)
    pos += len
    bytes
  }

  /**
   * Reads the remaining bytes from the buffer returns then as a byte array.
   */
  def readBytes(): Array[Byte] = {
    readBytes(buf.length - pos)
  }

  /**
   * Reads a UTF8-encoded byte series as a string.
   */
  def readUtf8String(): String = {
    val len = readVarInt()
    val bytes = readBytes(len)
    new String(bytes, Charset.UTF8)
  }

  /**
   * Reads a byte from the buffer.
   */
  def readByte(): Byte = {
    val v = buf(pos)
    pos += 1
    v
  }

  /**
   * Reads a fixed-length, 16-bit, big-endian short from the buffer.
   */
  def readShort(): Short = {
    val a = (buf(pos).toInt & 0xff) << 8
    pos += 1
    val b = buf(pos).toInt & 0xff
    pos += 1
    (a + b).toShort
  }

  /**
   * Reads a fixed-length, 32-bit, big-endian int from the buffer.
   */
  def readInt(): Int = {
    val n = ((buf(pos    ) & 255).toInt << 24) +
            ((buf(pos + 1) & 255).toInt << 16) +
            ((buf(pos + 2) & 255).toInt << 8) +
             (buf(pos + 3) & 255).toInt
    pos += 4
    n
  }

  /**
   * Reads a fixed-length, 64-bit, big-endian long from the buffer.
   */
  def readLong(): Long = {
    val n =  (buf(pos   ).toLong << 56) +
            ((buf(pos + 1) & 255).toLong << 48) +
            ((buf(pos + 2) & 255).toLong << 40) +
            ((buf(pos + 3) & 255).toLong << 32) +
            ((buf(pos + 4) & 255).toLong << 24) +
            ((buf(pos + 5) & 255) << 16) +
            ((buf(pos + 6) & 255) << 8) +
             (buf(pos + 7) & 255)
    pos += 8
    n
  }

  /**
   * Reads a varint-encoded 32-bit int from the buffer.
   */
  def readVarInt(): Int = {
    var b: Int = buf(pos) & 0xff
    pos += 1
    var n: Int = b & 0x7f
    if (b > 0x7f) {
      b = buf(pos) & 0xff
      pos += 1
      n ^= (b & 0x7f) << 7
      if (b > 0x7f) {
        b = buf(pos) & 0xff
        pos += 1
        n ^= (b & 0x7f) << 14
        if (b > 0x7f) {
          b = buf(pos) & 0xff
          pos += 1
          n ^= (b & 0x7f) << 21
          if (b > 0x7f) {
            b = buf(pos) & 0xff
            pos += 1
            n ^= (b & 0x7f) << 28
          }
          if (b > 0x7f) {
            throw new RuntimeException("Invalid int encoding")
          }
        }
      }
    }
    (n >>> 1) ^ -(n & 1) // back to two's-complement
  }

  /**
   * Reads a varint-encoded 64-bit long from the buffer.
   */
  def readVarLong(): Long = {
    var b: Long = buf(pos) & 0xff
    pos += 1
    var n: Long = b & 0x7f
    if (b > 0x7f) {
      b = buf(pos) & 0xff
      pos += 1
      n ^= (b & 0x7f) << 7
      if (b > 0x7f) {
        b = buf(pos) & 0xff
        pos += 1
        n ^= (b & 0x7f) << 14
        if (b > 0x7f) {
          b = buf(pos) & 0xff
          pos += 1
          n ^= (b & 0x7f) << 21
          if (b > 0x7f) {
            b = buf(pos) & 0xff
            pos += 1
            n ^= (b & 0x7f) << 28
            if (b > 0x7f) {
              b = buf(pos) & 0xff
              pos += 1
              n ^= (b & 0x7f) << 35
              if (b > 0x7f) {
                b = buf(pos) & 0xff
                pos += 1
                n ^= (b & 0x7f) << 42
                if (b > 0x7f) {
                  b = buf(pos) & 0xff
                  pos += 1
                  n ^= (b & 0x7f) << 49
                  if (b > 0x7f) {
                    b = buf(pos) & 0xff
                    pos += 1
                    n ^= (b & 0x7f) << 56
                    if (b > 0x7f) {
                      b = buf(pos) & 0xff
                      pos += 1
                      n ^= (b & 0x7f) << 63
                      if (b > 0x7f) {
                        throw new RuntimeException("Invalid long encoding")
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    (n >>> 1) ^ -(n & 1) // back to two's-complement
  }
}
