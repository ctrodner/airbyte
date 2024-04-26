/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.stream

import io.airbyte.cdk.discover.TableName
import io.airbyte.cdk.read.CursorColumn
import io.airbyte.cdk.read.SelectableState
import io.micronaut.context.annotation.DefaultImplementation

@DefaultImplementation(DefaultSelectQueryBuilder::class)
interface SelectQueryBuilder {

    fun sqlMaxCursorValue(table: TableName, cursorColumn: CursorColumn): String

    fun SelectableState.selectSpec(): SelectSpec
}
