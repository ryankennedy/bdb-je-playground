package com.unclehulka.bdbplayground.model

import com.codahale.jerkson.AST.JValue
import com.codahale.jerkson.JsonSnakeCase

@JsonSnakeCase
case class UpdatedData(updatedAt: Long, value: JValue)
