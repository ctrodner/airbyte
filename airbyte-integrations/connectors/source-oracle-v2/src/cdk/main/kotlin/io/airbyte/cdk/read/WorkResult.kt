/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read

/** The output of a [Worker]. */
data class WorkResult<S : Key, I : State<S>>(
    val input: I,
    val output: State<S>,
    val numRecords: Long = 0L
)
