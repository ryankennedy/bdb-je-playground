package com.unclehulka.bdbplayground.serialization

import java.lang.ref.SoftReference

/**
 * A buffer manager which maintains thread-local caches of byte arrays for reuse
 * by codecs.
 *
 * Stolen shamelessly from compress-lzf. Thanks, Tatu!
 */
object BufferRecycler {
  private[this] val recyclerRef = new ThreadLocal[SoftReference[BufferRecycler]]

  private[this] def recycler = {
    val ref = recyclerRef.get
    val recycler = if (ref == null) null else ref.get()
    if (recycler == null) {
      val br = new BufferRecycler
      recyclerRef.set(new SoftReference[BufferRecycler](br))
      br
    } else recycler
  }

  /**
   * Allocates a new buffer or reuses an old buffer with at least as many
   * elements as the specified minimum.
   */
  def allocBuffer(minSize: Int) = {
    recycler.allocBuffer(minSize)
  }

  /**
   * Releases the given buffer for reuse.
   */
  def releaseBuffer(buf: Array[Byte]) {
    recycler.releaseBuffer(buf)
  }

  private[serialization] def reset() {
    recycler.reset()
  }
}

private class BufferRecycler {
  private[this] val MinBufferSize = 16 * 1024
  private[this] var buffer: Array[Byte] = null

  def allocBuffer(minSize: Int) = {
    val buf = buffer
    if (buf == null || buf.length < minSize) {
      new Array[Byte](math.max(minSize, MinBufferSize))
    } else {
      reset()
      buf
    }
  }

  def releaseBuffer(buf: Array[Byte]) {
    if (buffer == null || buf.length > buffer.length) {
      buffer = buf
    }
  }

  def reset() {
    buffer = null
  }
}
