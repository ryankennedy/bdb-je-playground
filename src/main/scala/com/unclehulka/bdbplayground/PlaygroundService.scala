package com.unclehulka.bdbplayground

import com.codahale.fig.Configuration
import com.yammer.dropwizard.{Environment, Service}
import model.PlaygroundStore
import java.io.File
import com.sleepycat.je.{SecondaryConfig, DatabaseConfig, EnvironmentConfig}
import service.{ChangesResource, DataResource}

object PlaygroundService extends Service {
  def name: String = "playground"

  def configure(implicit config: Configuration, environment: Environment) {
    val dbDir = new File(config("db.dir").asRequired[String])
    assert(dbDir.exists() || dbDir.mkdirs())

    val envConfig = new EnvironmentConfig().setAllowCreate(true).setTransactional(true)
    val dbConfig = new DatabaseConfig().setAllowCreate(true).setTransactional(true)
    val sdbConfig = new SecondaryConfig()
    sdbConfig.setAllowCreate(true).setTransactional(true).setSortedDuplicates(true)

    val store = new PlaygroundStore(dbDir, envConfig, dbConfig, sdbConfig)
    environment.manage(store)

    environment.addResource(new DataResource(store))
    environment.addResource(new ChangesResource(store))
  }

  override def banner: Option[String] = Some("""
        _                                             _
       | |                                           | |
  _ __ | | __ _ _   _  __ _ _ __ ___  _   _ _ __   __| |
 | '_ \| |/ _` | | | |/ _` | '__/ _ \| | | | '_ \ / _` |
 | |_) | | (_| | |_| | (_| | | | (_) | |_| | | | | (_| |
 | .__/|_|\__,_|\__, |\__, |_|  \___/ \__,_|_| |_|\__,_|
 | |             __/ | __/ |
 |_|            |___/ |___/
  """)
}
