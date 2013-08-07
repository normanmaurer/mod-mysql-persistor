package me.normanmaurer.vertx.mods

/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.mysql.MySQLConnection
import org.vertx.java.busmods.BusModBase
import org.vertx.java.core.eventbus.Message
import org.vertx.java.core.Handler
import org.vertx.java.core.impl.EventLoopContext
import org.vertx.java.core.json.JsonObject

class MysqlPersistor extends BusModBase with Handler[Message[JsonObject]] {

  var connection :MySQLConnection

  override def start() {
    super.start()


    val address = getOptionalStringConfig("address", "vertx.mysqlpersistor")
    val configuration = URLParser.parse(createUrl)

    // Get access to the underlying EventLoop and pass it to MySQLConnection so no new threads are needed.
    val context = vertx.currentContext().asInstanceOf[EventLoopContext]
    val eventLoop = context.getEventLoop
    connection = new MySQLConnection(configuration = configuration, group = eventLoop)
    eb.registerHandler(address, this)
  }

  private def createUrl = {
    val username = getOptionalStringConfig("username", null)
    val password = getOptionalStringConfig("password", null)
    val host = getOptionalStringConfig("host", "localhost")
    val port = getOptionalIntConfig("port", 3306)
    val dbName = getMandatoryStringConfig("db_name")

    val sb = new StringBuilder
    sb.append("jdbc:mysql://").append(host).append(':').append(port).append("/").append(dbName)
    if (username != null && password != null) {
      sb.append('?').append("username=").append(username).append("&").append("password=").append(password)
    }
    sb.toString()
  }

  override def stop() {
    super.stop()
    connection.close
  }

  override def handle(message : Message[JsonObject]) {
    // TODO: Insert handling code :)
  }
}
