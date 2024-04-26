/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read

import io.airbyte.cdk.command.GlobalStateValue
import io.airbyte.cdk.command.StreamStateValue

/** Identifies the state of the READ operation for a given [Key]. */
sealed interface State<S : Key> {
    val key: S
}

sealed interface GlobalState : State<GlobalKey>

sealed interface StreamState : State<StreamKey>

/** This subset of states can be worked on by a [io.airbyte.cdk.read.stream.SelectWorker]. */
sealed interface SelectableState : StreamState

/** This subset of states can be represented in an Airbyte STATE message. */
sealed interface SerializableState<S : Key> : State<S>

sealed interface SerializableGlobalState : GlobalState, SerializableState<GlobalKey>

sealed interface SerializableStreamState : StreamState, SerializableState<StreamKey>

data class CdcNotStarted(override val key: GlobalKey) : GlobalState

data class CdcStarting(
    override val key: GlobalKey,
    val checkpoint: GlobalStateValue,
) : GlobalState

data class CdcOngoing(
    override val key: GlobalKey,
    val checkpoint: GlobalStateValue,
    val target: GlobalStateValue,
) : GlobalState, SerializableGlobalState

data class CdcCompleted(override val key: GlobalKey, val checkpoint: GlobalStateValue) :
    GlobalState, SerializableGlobalState

data class FullRefreshNotStarted(
    override val key: StreamKey,
) : StreamState

data class FullRefreshNonResumableStarting(
    override val key: StreamKey,
) : StreamState, SelectableState

data class FullRefreshResumableStarting(
    override val key: StreamKey,
    val primaryKey: List<DataColumn>
) : StreamState, SelectableState

data class FullRefreshResumableOngoing(
    override val key: StreamKey,
    val primaryKey: List<DataColumn>,
    val primaryKeyCheckpoint: List<String>,
) : StreamState, SelectableState, SerializableStreamState

data class FullRefreshCompleted(
    override val key: StreamKey,
) : StreamState, SerializableStreamState

data class CursorBasedNotStarted(
    override val key: StreamKey,
) : StreamState

data class CursorBasedInitialSyncStarting(
    override val key: StreamKey,
    val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val cursorCheckpoint: String,
) : StreamState, SelectableState

data class CursorBasedInitialSyncOngoing(
    override val key: StreamKey,
    val primaryKey: List<DataColumn>,
    val primaryKeyCheckpoint: List<String>,
    val cursor: CursorColumn,
    val cursorCheckpoint: String,
) : StreamState, SelectableState, SerializableStreamState

data class CursorBasedInitialSyncEmptyCompleted(
    override val key: StreamKey,
) : StreamState, SerializableStreamState

data class CursorBasedIncrementalStarting(
    override val key: StreamKey,
    val cursor: CursorColumn,
    val cursorCheckpoint: String,
) : StreamState

data class CursorBasedIncrementalOngoing(
    override val key: StreamKey,
    val cursor: CursorColumn,
    val cursorCheckpoint: String,
    val cursorTarget: String,
) : StreamState, SelectableState, SerializableStreamState

data class CursorBasedIncrementalCompleted(
    override val key: StreamKey,
    val cursor: CursorColumn,
    val cursorCheckpoint: String,
) : StreamState, SerializableStreamState

data class CdcInitialSyncNotStarted(
    override val key: StreamKey,
) : StreamState

data class CdcInitialSyncStarting(
    override val key: StreamKey,
    val primaryKey: List<DataColumn>,
) : StreamState, SelectableState

data class CdcInitialSyncOngoing(
    override val key: StreamKey,
    val primaryKey: List<DataColumn>,
    val primaryKeyCheckpoint: List<String>,
) : StreamState, SelectableState, SerializableStreamState

data class CdcInitialSyncCompleted(
    override val key: StreamKey,
) : StreamState, SerializableStreamState

fun SerializableGlobalState.toGlobalStateValue(): GlobalStateValue =
    when (this) {
        is CdcOngoing -> this.checkpoint
        is CdcCompleted -> this.checkpoint
    }

fun SerializableStreamState.toStreamStateValue(): StreamStateValue =
    when (this) {
        is FullRefreshResumableOngoing ->
            StreamStateValue(primaryKey.map { it.metadata.label }.zip(primaryKeyCheckpoint).toMap())
        is FullRefreshCompleted -> StreamStateValue()
        is CursorBasedInitialSyncOngoing ->
            StreamStateValue(
                primaryKey = primaryKey.map { it.metadata.label }.zip(primaryKeyCheckpoint).toMap(),
                cursors = mapOf(cursor.name to cursorCheckpoint)
            )
        is CursorBasedInitialSyncEmptyCompleted -> StreamStateValue()
        is CursorBasedIncrementalOngoing ->
            StreamStateValue(cursors = mapOf(cursor.name to cursorCheckpoint))
        is CursorBasedIncrementalCompleted ->
            StreamStateValue(cursors = mapOf(cursor.name to cursorCheckpoint))
        is CdcInitialSyncOngoing ->
            StreamStateValue(primaryKey.map { it.metadata.label }.zip(primaryKeyCheckpoint).toMap())
        is CdcInitialSyncCompleted -> StreamStateValue()
    }

fun CdcNotStarted.completed(checkpoint: GlobalStateValue) = CdcCompleted(key, checkpoint)

fun CdcStarting.ongoing(target: GlobalStateValue) = CdcOngoing(key, checkpoint, target)

fun CdcOngoing.ongoing(checkpoint: GlobalStateValue) = copy(checkpoint = checkpoint)

fun CdcOngoing.completed() = CdcCompleted(key, checkpoint)

fun FullRefreshNotStarted.starting(primaryKey: List<DataColumn>? = null): StreamState =
    if (primaryKey == null) startingNonResumable() else startingResumable(primaryKey)

fun FullRefreshNotStarted.startingNonResumable() = FullRefreshNonResumableStarting(key)

fun FullRefreshNotStarted.startingResumable(primaryKey: List<DataColumn>) =
    FullRefreshResumableStarting(key, primaryKey)

fun FullRefreshNonResumableStarting.completed() = FullRefreshCompleted(key)

fun FullRefreshResumableStarting.ongoing(primaryKeyCheckpoint: List<String>) =
    FullRefreshResumableOngoing(key, primaryKey, primaryKeyCheckpoint)

fun FullRefreshResumableStarting.completed() = FullRefreshCompleted(key)

fun FullRefreshResumableOngoing.ongoing(primaryKeyCheckpoint: List<String>) =
    copy(primaryKeyCheckpoint = primaryKeyCheckpoint)

fun FullRefreshResumableOngoing.completed() = FullRefreshCompleted(key)

fun CursorBasedNotStarted.starting(
    primaryKey: List<DataColumn>,
    cursor: CursorColumn,
    cursorCheckpoint: String,
) = CursorBasedInitialSyncStarting(key, primaryKey, cursor, cursorCheckpoint)

fun CursorBasedNotStarted.completed() = CursorBasedInitialSyncEmptyCompleted(key)

fun CursorBasedInitialSyncStarting.ongoing(primaryKeyCheckpoint: List<String>) =
    CursorBasedInitialSyncOngoing(key, primaryKey, primaryKeyCheckpoint, cursor, cursorCheckpoint)

fun CursorBasedInitialSyncStarting.completed() =
    CursorBasedIncrementalStarting(key, cursor, cursorCheckpoint)

fun CursorBasedInitialSyncOngoing.ongoing(primaryKeyCheckpoint: List<String>) =
    copy(primaryKeyCheckpoint = primaryKeyCheckpoint)

fun CursorBasedInitialSyncOngoing.completed() =
    CursorBasedIncrementalCompleted(key, cursor, cursorCheckpoint)

fun CursorBasedIncrementalStarting.ongoing(cursorTarget: String) =
    CursorBasedIncrementalOngoing(key, cursor, cursorCheckpoint, cursorTarget)

fun CursorBasedIncrementalOngoing.ongoing(cursorCheckpoint: String) =
    copy(cursorCheckpoint = cursorCheckpoint)

fun CursorBasedIncrementalOngoing.completed() =
    CursorBasedIncrementalCompleted(key, cursor, cursorCheckpoint)

fun CdcInitialSyncNotStarted.starting(primaryKey: List<DataColumn>) =
    CdcInitialSyncStarting(key, primaryKey)

fun CdcInitialSyncStarting.ongoing(primaryKeyCheckpoint: List<String>) =
    CdcInitialSyncOngoing(key, primaryKey, primaryKeyCheckpoint)

fun CdcInitialSyncStarting.completed() = CdcInitialSyncCompleted(key)

fun CdcInitialSyncOngoing.ongoing(primaryKeyCheckpoint: List<String>) =
    copy(primaryKeyCheckpoint = primaryKeyCheckpoint)

fun CdcInitialSyncOngoing.completed() = CdcInitialSyncCompleted(key)
