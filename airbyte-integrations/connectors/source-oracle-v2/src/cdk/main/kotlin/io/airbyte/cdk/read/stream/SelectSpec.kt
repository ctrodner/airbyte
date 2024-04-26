/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.stream

sealed interface SelectSpec {
    val sql: String
    val bindings: List<String>
    val dataCols: Int
    val stringColIdxs: List<Int>
}

data class SelectJustData(
    override val sql: String,
    override val bindings: List<String>,
    override val dataCols: Int
) : SelectSpec {
    override val stringColIdxs: List<Int>
        get() = listOf()
}

data class SelectWithPrimaryKey(
    override val sql: String,
    override val bindings: List<String>,
    override val dataCols: Int,
    val pkColIdxs: List<Int>,
) : SelectSpec {
    override val stringColIdxs: List<Int>
        get() = pkColIdxs
}

data class SelectWithCursor(
    override val sql: String,
    override val bindings: List<String>,
    override val dataCols: Int,
    val cursorColIdx: Int
) : SelectSpec {
    override val stringColIdxs: List<Int>
        get() = listOf(cursorColIdx)
}
