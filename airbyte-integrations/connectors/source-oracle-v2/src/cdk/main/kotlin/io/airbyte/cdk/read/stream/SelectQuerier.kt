/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.stream

interface SelectQuerier {
    fun SelectSpec.executeQuery(
        rowVisitor: (objects: Array<Any?>, strings: Array<String?>) -> Boolean,
    )
}
