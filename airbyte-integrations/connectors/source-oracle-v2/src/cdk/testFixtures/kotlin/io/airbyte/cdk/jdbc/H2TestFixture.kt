/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.jdbc

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import org.apache.commons.lang3.RandomStringUtils
import org.h2.tools.Server

private val log = KotlinLogging.logger {}

/** Wraps an H2 in-memory database and exposes a TCP server for it. */
@Singleton
class H2TestFixture(@Value("\${h2.database.name}") database: String? = null) : AutoCloseable {

    private val internalConnection: Connection
    private val server: Server

    val jdbcUrl: String
    val port: Int
    val database: String

    init {
        this.database = database ?: RandomStringUtils.randomAlphabetic(10).uppercase()
        internalConnection = DriverManager.getConnection("jdbc:h2:mem:${this.database}")
        server = Server.createTcpServer()
        server.start()
        jdbcUrl = "jdbc:h2:${server.url}/mem:${this.database}"
        port = server.port
        log.info { "H2 server ready to accept connections for $jdbcUrl" }
    }

    fun execute(sqlFmt: String, vararg args: Any?) {
        internalConnection.createStatement().use {
            it.execute(String.format(sqlFmt.replace('\n', ' '), *args))
        }
    }

    fun query(sqlFmt: String, vararg args: Any?): List<List<Any?>> =
        internalConnection.createStatement().use {
            val rs: ResultSet = it.executeQuery(String.format(sqlFmt.replace('\n', ' '), *args))
            val result = mutableListOf<List<Any?>>()
            val n: Int = rs.metaData.columnCount
            while (rs.next()) {
                val row = mutableListOf<Any?>()
                for (i in 1..n) {
                    row.add(rs.getObject(i)?.takeUnless { rs.wasNull() })
                }
                result.add(row)
            }
            result
        }

    override fun close() {
        log.info { "H2 server shutting down..." }
        server.stop()
        internalConnection.close()
        log.info { "H2 server shut down." }
    }
}
