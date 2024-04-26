package io.airbyte.cdk.read.stream

import io.airbyte.cdk.discover.TableName
import io.airbyte.cdk.read.CdcInitialSyncOngoing
import io.airbyte.cdk.read.CdcInitialSyncStarting
import io.airbyte.cdk.read.CursorBasedIncrementalOngoing
import io.airbyte.cdk.read.CursorBasedInitialSyncOngoing
import io.airbyte.cdk.read.CursorBasedInitialSyncStarting
import io.airbyte.cdk.read.CursorColumn
import io.airbyte.cdk.read.DataColumn
import io.airbyte.cdk.read.FullRefreshNonResumableStarting
import io.airbyte.cdk.read.FullRefreshResumableOngoing
import io.airbyte.cdk.read.FullRefreshResumableStarting
import io.airbyte.cdk.read.SelectableState
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

@Singleton
@Secondary
open class DefaultSelectQueryBuilder : SelectQueryBuilder {

    override fun sqlMaxCursorValue(table: TableName, cursorColumn: CursorColumn): String =
        "SELECT MAX(${cursorColumn.name}) FROM ${table.fullyQualifiedName()}"

    private fun TableName.fullyQualifiedName(): String =
        if (schema == null) name else "${schema}.${name}"

    override fun SelectableState.selectSpec(): SelectSpec {
        val dataColNames: List<String> = key.dataColumns.map { it.metadata.name }
        val select = "SELECT ${dataColNames.joinToString(", ")}"
        val from = "FROM ${key.table.fullyQualifiedName()}"
        val n: Int = key.dataColumns.size
        val pk: List<DataColumn> = primaryKey()
        val pkColNames: List<String> = pk.map { it.metadata.name }
        val pkWhere: String = pkColNames
            .mapIndexed { idx: Int, colName: String ->
                pkColNames.take(idx).map { "$it > ?" } + listOf("$colName = ?")
            }
            .map { it.joinToString(" AND ", "(", ")") }
            .joinToString(" OR ", "WHERE (", ")")
        val pkColIdxs: List<Int> = pk.map { key.dataColumns.indexOf(it)+1 }
        val pkBindings: List<String> = pkColIdxs
            .flatMapIndexed { idx: Int, colIdx: Int -> pkColIdxs.take(idx) + listOf(colIdx) }
            .mapNotNull { colIdx: Int -> primaryKeyCheckpoint().getOrNull(colIdx-1) }
            .toList()
        val pkOrder = "ORDER BY ${pkColNames.joinToString(", ")}"
        return when(this) {
            is CdcInitialSyncStarting ->
                SelectWithPrimaryKey("$select $from $pkOrder", listOf(), n, pkColIdxs)
            is CdcInitialSyncOngoing ->
                SelectWithPrimaryKey("$select $from $pkWhere $pkOrder", pkBindings, n, pkColIdxs)
            is CursorBasedInitialSyncStarting -> {
                val sql = "$select $from WHERE ${cursor.name} < ? $pkOrder"
                val bindings: List<String> = listOf(cursorCheckpoint)
                SelectWithPrimaryKey(sql, bindings, n, pkColIdxs)
            }
            is CursorBasedInitialSyncOngoing -> {
                val sql = "$select $from $pkWhere AND ${cursor.name} < ? $pkOrder"
                val bindings: List<String> = pkBindings + listOf(cursorCheckpoint)
                SelectWithPrimaryKey(sql, bindings, n, pkColIdxs)
            }
            is CursorBasedIncrementalOngoing -> {
                val where = "WHERE ${cursor.name} > ? AND ${cursor.name} < ?"
                val order = "ORDER BY ${cursor.name}"
                val bindings: List<String> = listOf(cursorCheckpoint, cursorTarget)
                val cursorColIdx: Int = 1 + key.dataColumns.indexOfFirst {
                    it.metadata.name == cursor.name
                }
                if (cursorColIdx == 0) {
                    val sql = "$select, ${cursor.name} $from $where $order"
                    SelectWithCursor(sql, bindings, n, 1+n)
                } else {
                    val sql = "$select $from $where $order"
                    SelectWithCursor(sql, bindings, n, cursorColIdx)
                }
            }
            is FullRefreshNonResumableStarting ->
                SelectJustData("$select $from", listOf(), n)
            is FullRefreshResumableStarting ->
                SelectWithPrimaryKey("$select $from $pkOrder", listOf(), n, pkColIdxs)
            is FullRefreshResumableOngoing ->
                SelectWithPrimaryKey("$select $from $pkWhere $pkOrder", pkBindings, n, pkColIdxs)
        }
    }

    private fun SelectableState.primaryKey(): List<DataColumn> = when (this) {
        is CdcInitialSyncStarting -> primaryKey
        is CdcInitialSyncOngoing -> primaryKey
        is CursorBasedInitialSyncStarting -> primaryKey
        is CursorBasedInitialSyncOngoing -> primaryKey
        is CursorBasedIncrementalOngoing -> listOf()
        is FullRefreshNonResumableStarting -> listOf()
        is FullRefreshResumableStarting -> primaryKey
        is FullRefreshResumableOngoing -> primaryKey
    }

    private fun SelectableState.primaryKeyCheckpoint(): List<String> = when (this) {
        is CdcInitialSyncOngoing -> primaryKeyCheckpoint
        is CursorBasedInitialSyncOngoing -> primaryKeyCheckpoint
        is FullRefreshResumableOngoing -> primaryKeyCheckpoint
        else -> listOf()
    }
}
