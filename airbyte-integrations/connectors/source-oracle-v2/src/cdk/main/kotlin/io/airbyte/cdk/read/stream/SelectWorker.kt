/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.stream

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.read.CdcInitialSyncOngoing
import io.airbyte.cdk.read.CdcInitialSyncStarting
import io.airbyte.cdk.read.CursorBasedIncrementalOngoing
import io.airbyte.cdk.read.CursorBasedInitialSyncOngoing
import io.airbyte.cdk.read.CursorBasedInitialSyncStarting
import io.airbyte.cdk.read.FullRefreshNonResumableStarting
import io.airbyte.cdk.read.FullRefreshResumableOngoing
import io.airbyte.cdk.read.FullRefreshResumableStarting
import io.airbyte.cdk.read.SelectableState
import io.airbyte.cdk.read.StreamKey
import io.airbyte.cdk.read.StreamState
import io.airbyte.cdk.read.WorkResult
import io.airbyte.cdk.read.Worker
import io.airbyte.cdk.read.completed
import io.airbyte.cdk.read.ongoing
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import java.util.concurrent.atomic.AtomicBoolean

class SelectWorker(
    queryBuilder: SelectQueryBuilder,
    querier: SelectQuerier,
    serializerFactory: SelectWorkerSerializer.Factory,
    val outputConsumer: OutputConsumer,
    override val input: SelectableState,
) : SelectQueryBuilder by queryBuilder, SelectQuerier by querier, Worker<StreamKey, StreamState> {

    private val stopping = AtomicBoolean()

    override fun signalStop() {
        stopping.set(true)
    }

    val spec: SelectSpec = input.selectSpec()
    val serializer: SelectWorkerSerializer = serializerFactory.make(key)

    override fun call(): WorkResult<StreamKey, StreamState> {
        var numRecords = 0L
        val pk: Array<String?> =
            when (spec) {
                is SelectWithPrimaryKey -> Array(spec.pkColIdxs.size) { null }
                is SelectJustData,
                is SelectWithCursor -> Array(0) { null }
            }
        var cursor: String? = null
        spec.executeQuery { rowObjects: Array<Any?>, rowStrings: Array<String?> ->
            outputConsumer.accept(
                AirbyteRecordMessage()
                    .withStream(key.name)
                    .withNamespace(key.namespace)
                    .withData(serializer.serialize(rowObjects))
            )
            System.arraycopy(rowStrings, 0, pk, 0, pk.size)
            cursor = rowStrings.firstOrNull()
            numRecords++
            return@executeQuery stopping.get()
        }
        val pkCheckpoint: List<String> by lazy {
            pk.toList().filterNotNull().also {
                if (it.size < pk.size) {
                    throw IllegalStateException("primary key values contain NULL: ${pk.toList()}")
                }
            }
        }
        val cursorCheckpoint: String by lazy {
            cursor ?: throw IllegalStateException("NULL cursor column value")
        }
        if (numRecords == 0L) {
            val completed: StreamState =
                when (input) {
                    is FullRefreshNonResumableStarting -> input.completed()
                    is FullRefreshResumableStarting -> input.completed()
                    is FullRefreshResumableOngoing -> input.completed()
                    is CursorBasedInitialSyncStarting -> input.completed()
                    is CursorBasedInitialSyncOngoing -> input.completed()
                    is CursorBasedIncrementalOngoing -> input.completed()
                    is CdcInitialSyncStarting -> input.completed()
                    is CdcInitialSyncOngoing -> input.completed()
                }
            return WorkResult(input, completed)
        }
        val output: StreamState =
            when (input) {
                is FullRefreshNonResumableStarting -> input.completed()
                is FullRefreshResumableStarting -> input.ongoing(pkCheckpoint)
                is FullRefreshResumableOngoing -> input.ongoing(pkCheckpoint)
                is CursorBasedInitialSyncStarting -> input.ongoing(pkCheckpoint)
                is CursorBasedInitialSyncOngoing -> input.ongoing(pkCheckpoint)
                is CursorBasedIncrementalOngoing -> input.ongoing(cursorCheckpoint)
                is CdcInitialSyncStarting -> input.ongoing(pkCheckpoint)
                is CdcInitialSyncOngoing -> input.ongoing(pkCheckpoint)
            }
        return WorkResult(input, output, numRecords)
    }
}
