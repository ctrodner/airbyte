/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.read.global.CdcColdStartWorker
import io.airbyte.cdk.read.global.CdcOngoingWorker
import io.airbyte.cdk.read.global.CdcWarmStartWorker
import io.airbyte.cdk.read.stream.CdcInitialSyncPrepWorker
import io.airbyte.cdk.read.stream.CursorBasedColdStartWorker
import io.airbyte.cdk.read.stream.CursorBasedWarmStartWorker
import io.airbyte.cdk.read.stream.FullRefreshPrepWorker
import io.airbyte.cdk.read.stream.SelectQuerier
import io.airbyte.cdk.read.stream.SelectQueryBuilder
import io.airbyte.cdk.read.stream.SelectWorker
import io.airbyte.cdk.read.stream.SelectWorkerSerializer
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** Executes [Worker]s sequentially for a particular [Key]. */
class WorkerThreadRunnable(
    private val queryBuilder: SelectQueryBuilder,
    private val querier: SelectQuerier,
    private val serializerFactory: SelectWorkerSerializer.Factory,
    private val outputConsumer: OutputConsumer,
    private val workUnitTimeout: Duration,
    private val stateManager: StateManager,
    private var state: State<out Key>,
) : Runnable {

    val name: String =
        "worker-" +
            when (val key = state.key) {
                is GlobalKey -> "global"
                is StreamKey -> key.namePair.toString()
            }

    private val log = KotlinLogging.logger {}

    private val ex: ExecutorService = Executors.newSingleThreadExecutor { Thread(it, name) }

    override fun run() {
        log.info { "$name: new state machine execution" }
        while (true) {
            log.info { "$name: processing state $state" }
            val worker: Worker<*, *> = worker(state) ?: break
            log.info { "$name: calling ${worker.javaClass.simpleName}" }
            val future: Future<out WorkResult<*, *>> = ex.submit(worker)
            val result: WorkResult<*, *> =
                try {
                    future.get(workUnitTimeout.toMillis(), TimeUnit.MILLISECONDS)
                } catch (_: TimeoutException) {
                    log.info { "$name: ${worker.javaClass.simpleName} soft timeout" }
                    worker.signalStop()
                    future.get()
                }
            log.info { "$name: ${result.numRecords} produced by $worker" }
            state = result.output
            when (result.output) {
                is SerializableGlobalState -> stateManager.set(result.output, result.numRecords)
                is SerializableStreamState -> stateManager.set(result.output, result.numRecords)
                else -> continue
            }
            val checkpoint: List<AirbyteStateMessage> = stateManager.checkpoint()
            log.info { "$name: checkpoint of ${checkpoint.size} state message(s)" }
            checkpoint.forEach(outputConsumer::accept)
        }
        log.info { "$name: reached terminal state $state" }
    }

    private fun worker(input: State<out Key>): Worker<*, *>? =
        when (input) {
            is CdcCompleted -> null
            is CdcOngoing -> CdcOngoingWorker(input)
            is CdcStarting -> CdcWarmStartWorker(input)
            is CdcNotStarted -> CdcColdStartWorker(input)
            is CursorBasedIncrementalCompleted -> null
            is CursorBasedIncrementalStarting ->
                CursorBasedWarmStartWorker(queryBuilder, querier, input)
            is CursorBasedInitialSyncEmptyCompleted -> null
            is CursorBasedNotStarted -> CursorBasedColdStartWorker(queryBuilder, querier, input)
            is CdcInitialSyncCompleted -> null
            is CdcInitialSyncNotStarted -> CdcInitialSyncPrepWorker(input)
            is FullRefreshCompleted -> null
            is FullRefreshNotStarted -> FullRefreshPrepWorker(input)
            is SelectableState ->
                SelectWorker(queryBuilder, querier, serializerFactory, outputConsumer, input)
        }
}
