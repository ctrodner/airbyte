/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.jdbc

import io.airbyte.cdk.read.stream.SelectQuerier
import io.airbyte.cdk.read.stream.SelectSpec
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

private val log = KotlinLogging.logger {}

@Secondary
class JdbcSelectQuerier(
    private val jdbcConnectionFactory: JdbcConnectionFactory,
) : SelectQuerier {

    override fun SelectSpec.executeQuery(
        rowVisitor: (objects: Array<Any?>, strings: Array<String?>) -> Boolean
    ) {
        jdbcConnectionFactory.get().use { conn: Connection ->
            conn.prepareStatement(sql).use { stmt: PreparedStatement ->
                bindings.forEachIndexed { idx: Int, value: String ->
                    stmt.setString(idx + 1, value)
                }
                val objects = Array<Any?>(dataCols) { null }
                val strings = Array<String?>(stringColIdxs.size) { null }
                val rs: ResultSet = stmt.executeQuery()
                while (rs.next()) {
                    setObjects(rs, objects)
                    setStrings(rs, strings)
                    if (rowVisitor(objects, strings)) break
                }
            }
        }
    }

    private fun SelectSpec.setObjects(rs: ResultSet, objects: Array<Any?>) {
        for (colIdx in 1..dataCols) {
            var v: Any? = null
            try {
                v = rs.getObject(colIdx)
            } catch (e: SQLException) {
                // This is required because some JDBC drivers have bugs.
                log.trace(e) { "falling back to getString to access column $colIdx in $sql" }
                try {
                    v = rs.getString(colIdx)
                } catch (e: SQLException) {
                    log.trace(e) {
                        "failed both getObject and getString for column $colIdx in $sql"
                    }
                }
            }
            objects[colIdx - 1] = v.takeUnless { rs.wasNull() }
        }
    }

    private fun SelectSpec.setStrings(rs: ResultSet, strings: Array<String?>) {
        for (colIdx in stringColIdxs) {
            var v: String? = null
            try {
                v = rs.getString(colIdx)
            } catch (e: SQLException) {
                // This is required because some JDBC drivers have bugs.
                log.trace(e) { "failed getString for column $colIdx in $sql" }
            }
            strings[colIdx - 1] = v.takeUnless { rs.wasNull() }
        }
    }
}
