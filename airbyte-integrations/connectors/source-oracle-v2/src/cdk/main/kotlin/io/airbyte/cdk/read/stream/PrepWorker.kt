/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.stream

import io.airbyte.cdk.read.CdcInitialSyncNotStarted
import io.airbyte.cdk.read.CursorBasedIncrementalStarting
import io.airbyte.cdk.read.CursorBasedNotStarted
import io.airbyte.cdk.read.CursorColumn
import io.airbyte.cdk.read.DataColumn
import io.airbyte.cdk.read.FullRefreshNotStarted
import io.airbyte.cdk.read.StreamKey
import io.airbyte.cdk.read.StreamState
import io.airbyte.cdk.read.WorkResult
import io.airbyte.cdk.read.Worker
import io.airbyte.cdk.read.completed
import io.airbyte.cdk.read.ongoing
import io.airbyte.cdk.read.starting

sealed class PrepWorker<I : StreamState> : Worker<StreamKey, I> {

    override fun signalStop() {} // unstoppable
}

class CdcInitialSyncPrepWorker(override val input: CdcInitialSyncNotStarted) :
    PrepWorker<CdcInitialSyncNotStarted>() {

    override fun call(): WorkResult<StreamKey, CdcInitialSyncNotStarted> {
        val primaryKey: List<DataColumn> =
            key.configuredPrimaryKey
                ?: key.primaryKeyCandidates.firstOrNull()
                    ?: throw IllegalStateException("Stream has no primary key.")
        return WorkResult(input, input.starting(primaryKey))
    }
}

class FullRefreshPrepWorker(override val input: FullRefreshNotStarted) :
    PrepWorker<FullRefreshNotStarted>() {

    override fun call(): WorkResult<StreamKey, FullRefreshNotStarted> {
        val maybePrimaryKey: List<DataColumn>? =
            key.configuredPrimaryKey ?: key.primaryKeyCandidates.firstOrNull()
        return WorkResult(input, input.starting(maybePrimaryKey))
    }
}

class CursorBasedColdStartWorker(
    val selectQueryBuilder: SelectQueryBuilder,
    q: SelectQuerier,
    override val input: CursorBasedNotStarted
) : SelectQuerier by q, MaxCursorValueQuerier, PrepWorker<CursorBasedNotStarted>() {

    override fun call(): WorkResult<StreamKey, CursorBasedNotStarted> {
        val primaryKey: List<DataColumn> =
            key.configuredPrimaryKey
                ?: key.primaryKeyCandidates.firstOrNull()
                    ?: throw IllegalStateException("Stream has no primary key.")
        val cursor: CursorColumn =
            key.configuredCursor
                ?: key.cursorCandidates.firstOrNull()
                    ?: throw IllegalStateException("Stream has no cursor.")
        val sql: String = selectQueryBuilder.sqlMaxCursorValue(key.table, cursor)
        val maxCursorValue: String? = queryMaxCursorValue(cursor, sql)
        return WorkResult(
            input,
            when (maxCursorValue) {
                null -> input.completed()
                else -> input.starting(primaryKey, cursor, maxCursorValue)
            }
        )
    }
}

class CursorBasedWarmStartWorker(
    selectQueryBuilder: SelectQueryBuilder,
    q: SelectQuerier,
    override val input: CursorBasedIncrementalStarting,
) : SelectQuerier by q, MaxCursorValueQuerier, PrepWorker<CursorBasedIncrementalStarting>() {

    val sqlMaxCursorValue: String = selectQueryBuilder.sqlMaxCursorValue(key.table, input.cursor)

    override fun call(): WorkResult<StreamKey, CursorBasedIncrementalStarting> {
        val maxCursorValue: String =
            queryMaxCursorValue(input.cursor, sqlMaxCursorValue)
                ?: throw IllegalStateException("Stream has no cursor data")
        return WorkResult(input, input.ongoing(maxCursorValue))
    }
}

sealed interface MaxCursorValueQuerier : SelectQuerier {

    fun queryMaxCursorValue(cursor: CursorColumn, sql: String): String? {
        var result: String? = null
        val spec = SelectWithCursor(sql, listOf(), 0, 1)
        spec.executeQuery { _, strings: Array<String?> ->
            result =
                strings[0] ?: throw IllegalStateException("Cursor ${cursor.name} has NULL value.")
            true
        }
        return result
    }
}
