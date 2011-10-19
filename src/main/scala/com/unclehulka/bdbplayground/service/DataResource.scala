package com.unclehulka.bdbplayground.service

import com.unclehulka.bdbplayground.model.PlaygroundStore
import javax.ws.rs._
import core.{MediaType, Response}
import com.codahale.jerkson.AST.JValue
import com.codahale.jerkson.Json
import com.codahale.logula.Logging

@Path("/data/{assetPath:(.+)}")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class DataResource(store: PlaygroundStore) extends Logging {
  @GET
  def get(@PathParam("assetPath") path: String) = {
    log.trace("GET /%s", path)
    store.get(path) match {
      case Some(value) => Response.ok(Json.generate(value)).build()
      case None => Response.status(Response.Status.NOT_FOUND)
                           .entity("Object %s not found".format(path))
                           .`type`(MediaType.TEXT_PLAIN)
                           .build()
    }
  }

  @PUT
  def put(@PathParam("assetPath") path: String, value: JValue) = {
    log.trace("PUT /%s", path)
    if (store.set(path, value)) {
      Response.noContent().build()
    } else {
      Response.serverError()
              .entity("Failed to write object %s".format(path))
              .`type`(MediaType.TEXT_PLAIN)
              .build()
    }
  }

  @DELETE
  def delete(@PathParam("assetPath") path: String) = {
    log.trace("DELETE /%s", path)
    store.remove(path)
    Response.noContent().build()
  }
}
