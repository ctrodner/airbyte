/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.stream

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.read.StreamKey
import io.airbyte.commons.json.Jsons
import io.micronaut.context.annotation.DefaultImplementation


class SelectWorkerSerializer(val fieldSerializers: List<FieldSerializer>) {

    fun serialize(data: Array<Any?>): JsonNode {
        val objectNode: ObjectNode = Jsons.emptyObject() as ObjectNode
        fieldSerializers.forEachIndexed { idx: Int, fieldSerializer: FieldSerializer ->
            objectNode.set<ObjectNode>(fieldSerializer.fieldName, fieldSerializer.fn(data[idx]))
        }
        return objectNode
    }

    class FieldSerializer(val fieldName: String, val fn: (Any?) -> JsonNode)

    @DefaultImplementation(DefaultSelectWorkerSerializerFactory::class)
    interface Factory {
        fun make(key: StreamKey): SelectWorkerSerializer
    }
}
