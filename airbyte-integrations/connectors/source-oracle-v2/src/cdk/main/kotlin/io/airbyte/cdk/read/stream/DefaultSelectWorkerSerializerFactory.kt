package io.airbyte.cdk.read.stream

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.cdk.discover.ArrayColumnType
import io.airbyte.cdk.discover.ColumnType
import io.airbyte.cdk.discover.LeafType
import io.airbyte.cdk.read.DataColumn
import io.airbyte.cdk.read.StreamKey
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.chrono.ChronoLocalDate
import java.time.format.DateTimeFormatter

@Singleton
@Secondary
open class DefaultSelectWorkerSerializerFactory : SelectWorkerSerializer.Factory {

    override fun make(key: StreamKey) =
        SelectWorkerSerializer(key.dataColumns.map {
            SelectWorkerSerializer.FieldSerializer(it.metadata.name, buildMapper(it))
        })

    fun buildMapper(dataColumn: DataColumn): (Any?) -> JsonNode {
        val mapperInner = buildMapperRecursive(dataColumn.type)
        return { v: Any? ->
            try {
                mapperInner(v)
            } catch (e: Exception) {
                throw RuntimeException("${dataColumn.metadata.name} value $v not suitable " +
                    "for ${dataColumn.type}", e)
            }
        }
    }

    fun buildMapperRecursive(type: ColumnType): (Any?) -> JsonNode {
        when(type) {
            is LeafType -> return { v: Any? ->
                type.map(v)
            }
            is ArrayColumnType -> {
                val itemMapper = buildMapperRecursive(type.item)
                return { v: Any? ->
                    Jsons.arrayNode().apply {
                        for (e in mapToList(v)) {
                            add(itemMapper(e))
                        }
                    }
                }
            }
        }
    }

    fun mapToList(value: Any?): List<Any?> = when (value) {
        null -> listOf()
        is java.sql.Array -> (value.array as Iterable<*>).toList()
        is ArrayNode -> value.iterator().asSequence().toList()
        is Iterable<*> -> value.toList()
        else -> throw IllegalArgumentException("cannot process $value as an array type")
    }

    fun LeafType.map(value: Any?): JsonNode = when (this) {
        LeafType.NULL -> json.nullNode()
        LeafType.STRING -> when (value) {
            null -> json.nullNode()
            else -> json.textNode(value.toString())
        }

        LeafType.BINARY -> when (value) {
            null -> json.nullNode()
            is ByteArray -> json.binaryNode(value)
            else -> throw IllegalArgumentException("unsupported type ${value.javaClass}")
        }

        LeafType.JSONB -> when (value) {
            null -> json.nullNode()
            is JsonNode -> value
            is ByteArray -> Jsons.deserialize(value)
            is CharSequence -> Jsons.deserialize(value.toString())
            else -> Jsons.jsonNode(value)
        }

        LeafType.BOOLEAN -> when (value) {
            null -> json.nullNode()
            is Boolean -> json.booleanNode(value)
            else -> json.booleanNode(json.textNode(value.toString()).asBoolean())
        }

        LeafType.INTEGER -> when (value) {
            null -> json.nullNode()
            is Byte -> json.numberNode(value)
            is Short -> json.numberNode(value)
            is Int -> json.numberNode(value)
            is Long -> json.numberNode(value)
            is Float -> json.numberNode(
                java.math.BigDecimal.valueOf(value.toDouble()).toBigInteger()
            )

            is Double -> json.numberNode(java.math.BigDecimal.valueOf(value).toBigInteger())
            is java.math.BigInteger -> json.numberNode(value)
            is java.math.BigDecimal -> json.numberNode(value.toBigInteger())
            else -> json.numberNode(java.math.BigInteger(value.toString()))
        }

        LeafType.NUMBER -> when (value) {
            null -> json.nullNode()
            is Byte -> json.numberNode(value)
            is Short -> json.numberNode(value)
            is Int -> json.numberNode(value)
            is Long -> json.numberNode(value)
            is Float -> json.numberNode(value)
            is Double -> json.numberNode(value)
            is java.math.BigInteger -> json.numberNode(value)
            is java.math.BigDecimal -> json.numberNode(value)
            else -> json.numberNode(java.math.BigDecimal(value.toString()))
        }

        LeafType.DATE -> when (value) {
            null -> json.nullNode()
            is java.sql.Date -> json.textNode(value.toLocalDate().format(dateFormatter))
            is LocalDate -> json.textNode(value.format(dateFormatter))
            is LocalDateTime -> json.textNode(value.format(dateFormatter))
            is ChronoLocalDate -> json.textNode(value.format(dateFormatter))
            is OffsetDateTime -> json.textNode(value.format(dateFormatter))
            is ZonedDateTime -> json.textNode(value.format(dateFormatter))
            else -> json.textNode(value.toString())
        }

        LeafType.TIME_WITH_TIMEZONE -> when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timeTzFormatter))
            is LocalTime -> json.textNode(value.format(timeTzFormatter))
            is LocalDateTime -> json.textNode(value.format(timeTzFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timeTzFormatter))
            is OffsetDateTime -> json.textNode(value.format(timeTzFormatter))
            is OffsetTime -> json.textNode(value.format(timeTzFormatter))
            is ZonedDateTime -> json.textNode(value.format(timeTzFormatter))
            else -> json.textNode(value.toString())
        }

        LeafType.TIME_WITHOUT_TIMEZONE -> when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timeFormatter))
            is LocalTime -> json.textNode(value.format(timeFormatter))
            is LocalDateTime -> json.textNode(value.format(timeFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timeFormatter))
            is OffsetDateTime -> json.textNode(value.format(timeFormatter))
            is OffsetTime -> json.textNode(value.format(timeFormatter))
            is ZonedDateTime -> json.textNode(value.format(timeFormatter))
            else -> json.textNode(value.toString())
        }

        LeafType.TIMESTAMP_WITH_TIMEZONE -> when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timestampTzFormatter))
            is LocalTime -> json.textNode(value.format(timestampTzFormatter))
            is LocalDateTime -> json.textNode(value.format(timestampTzFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timestampTzFormatter))
            is OffsetDateTime -> json.textNode(value.format(timestampTzFormatter))
            is OffsetTime -> json.textNode(value.format(timestampTzFormatter))
            is ZonedDateTime -> json.textNode(value.format(timestampTzFormatter))
            else -> json.textNode(value.toString())
        }

        LeafType.TIMESTAMP_WITHOUT_TIMEZONE -> when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timestampFormatter))
            is LocalTime -> json.textNode(value.format(timestampFormatter))
            is LocalDateTime -> json.textNode(value.format(timestampFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timestampFormatter))
            is OffsetDateTime -> json.textNode(value.format(timestampFormatter))
            is OffsetTime -> json.textNode(value.format(timestampFormatter))
            is ZonedDateTime -> json.textNode(value.format(timestampFormatter))
            else -> json.textNode(value.toString())
        }
    }

    companion object {
        val json: JsonNodeFactory =
            MoreMappers.initMapper().nodeFactory
        val dateFormatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val timeFormatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
        val timeTzFormatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSXXX")
        val timestampFormatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        val timestampTzFormatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
    }

}


