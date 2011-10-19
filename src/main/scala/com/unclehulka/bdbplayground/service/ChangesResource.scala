package com.unclehulka.bdbplayground.service

import com.unclehulka.bdbplayground.model.PlaygroundStore
import javax.ws.rs.{Produces, PathParam, GET, Path}
import javax.ws.rs.core.{MediaType, Response}

@Path("/changes/{ts:(\\d+)}")
@Produces(Array(MediaType.APPLICATION_JSON))
class ChangesResource(store: PlaygroundStore) {
  @GET
  def get(@PathParam("ts") ts: Long) = {
    store.changesSince(ts) match {
      case Some(value) => Response.ok(Array(value)).build()
      case None => Response.ok(Array.empty).build()
    }
  }
}
