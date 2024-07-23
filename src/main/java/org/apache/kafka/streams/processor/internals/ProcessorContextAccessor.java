package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;

import static org.apache.kafka.streams.processor.internals.InternalProcessorContext.BYTEARRAY_VALUE_SERIALIZER;
import static org.apache.kafka.streams.processor.internals.InternalProcessorContext.BYTES_KEY_SERIALIZER;

public class ProcessorContextAccessor {
    public static void logChange(final String storeName,
                                 final Bytes key,
                                 final byte[] value,
                                 final long timestamp,
                                 final Headers headers,
                                 ProcessorContextImpl context) {
        throwUnsupportedOperationExceptionIfStandby("logChange", context);

        final TopicPartition changelogPartition = context.stateManager().registeredChangelogPartitionFor(storeName);

        context.recordCollector().send(
                changelogPartition.topic(),
                key,
                value,
                headers,
                changelogPartition.partition(),
                timestamp,
                BYTES_KEY_SERIALIZER,
                BYTEARRAY_VALUE_SERIALIZER
        );
    }

    private static void throwUnsupportedOperationExceptionIfStandby(final String operationName, ProcessorContextImpl context) {
        if (context.taskType() == Task.TaskType.STANDBY) {
            throw new UnsupportedOperationException(
                    "this should not happen: " + operationName + "() is not supported in standby tasks.");
        }
    }
}
