package com.unclehulka.bdbplayground.model

import com.yammer.dropwizard.lifecycle.Managed
import java.util.concurrent.atomic.AtomicReference
import com.sleepycat.je._
import com.codahale.logula.Logging
import com.codahale.jerkson.Json
import com.codahale.jerkson.AST.JValue
import java.io.File
import com.unclehulka.bdbplayground.serialization.{ReadBuffer, WriteBuffer}

class PlaygroundStore(dbDir: File,
                      envConfig: EnvironmentConfig,
                      dbConfig: DatabaseConfig,
                      sdbConfig: SecondaryConfig) extends Managed with Logging {

  require(dbDir.exists() && dbDir.isDirectory)

  private val handles = new AtomicReference[(Environment, Database, SecondaryDatabase)]()
  sdbConfig.setKeyCreator(TimestampKeyCreator)

  def get(key: String) = {
    Option(handles.get()).flatMap { case (env, db, sdb) =>
      val keyEntry = new DatabaseEntry(key.getBytes("utf-8"))
      val valueEntry = new DatabaseEntry()
      db.get(null, keyEntry, valueEntry, null) match {
        case OperationStatus.SUCCESS => {
          val buffer = new ReadBuffer(valueEntry.getData)
          buffer.readVarLong()
          Some(Json.parse[JValue](buffer.readUtf8String()))
        }
        case OperationStatus.NOTFOUND => None
      }
    }
  }

  def set(key: String, value: JValue) = {
    Option(handles.get()).map { case (env, db, sdb) =>
      val keyEntry = new DatabaseEntry(key.getBytes("utf-8"))

      val buffer = new WriteBuffer(16 * 1024)
      buffer.writeVarLong(System.currentTimeMillis())
      buffer.writeUtf8String(Json.generate(value))
      val valueEntry = new DatabaseEntry(buffer.finish())

      val txn = env.beginTransaction(null, null)
      try {
        db.delete(txn, keyEntry)
        db.put(txn, keyEntry, valueEntry) match {
          case OperationStatus.SUCCESS => {
            txn.commit()
            true
          }
          case other => {
            txn.abort()
            false
          }
        }
      }
      catch {
        case e => {
          log.error(e, "Error setting key %s", key)
          txn.abort()
          throw e
        }
      }
    }.getOrElse(false)
  }

  def remove(key: String) {
    Option(handles.get()).foreach { case (env, db, sdb) =>
      val keyEntry = new DatabaseEntry(key.getBytes("utf-8"))
      db.delete(null, keyEntry)
    }
  }

  def changesSince(ts: Long) = {
    Option(handles.get()).flatMap { case (env, db, sdb) =>
      val cursorConfig = new CursorConfig().setReadUncommitted(true)
      val cursor = sdb.openCursor(null, cursorConfig)

      val writeBuffer = new WriteBuffer(8)
      writeBuffer.writeLong(ts)
      val keyEntry = new DatabaseEntry(writeBuffer.finish())
      val valueEntry = new DatabaseEntry()
      cursor.getSearchKeyRange(keyEntry, valueEntry, null) match {
        case OperationStatus.SUCCESS => {
          val readBuffer = new ReadBuffer(valueEntry.getData)
          val ts = readBuffer.readVarLong()
          Some(UpdatedData(ts, Json.parse[JValue](readBuffer.readUtf8String())))
        }
        case OperationStatus.NOTFOUND => {
          None
        }
      }
    }
  }

  override def start() {
    val env = new Environment(dbDir, envConfig)
    val txn = env.beginTransaction(null, null)

    try {
      val db = env.openDatabase(txn, "objects", dbConfig)
      val sdb = env.openSecondaryDatabase(txn, "objects-sequence", db, sdbConfig)
      txn.commit()
      handles.set((env, db, sdb))
    } catch {
      case e => {
        txn.abort()
        log.fatal(e, "Error opening databases")
        throw e
      }
    }
  }

  override def stop() {
    Option(handles.get()).foreach { case (env, db, sdb) =>
      try {
        sdb.close()
        log.debug("Secondary database closed")
      }
      catch {
        case e => log.warn(e, "Error closing secondary database")
      }

      try {
        db.close()
        log.debug("Primary database closed")
      }
      catch {
        case e => log.warn(e, "Error closing primary database")
      }

      try {
        env.close()
        log.debug("Environment closed")
      }
      catch {
        case e => log.warn(e, "Error closing environment")
      }
    }
  }
}
