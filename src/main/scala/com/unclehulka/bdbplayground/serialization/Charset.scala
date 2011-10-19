package com.unclehulka.bdbplayground.serialization

import java.nio.charset.{Charset => jnioCharset}

object Charset {
  val ASCII = jnioCharset.forName("ASCII")
  val UTF8 = jnioCharset.forName("UTF-8")
}
