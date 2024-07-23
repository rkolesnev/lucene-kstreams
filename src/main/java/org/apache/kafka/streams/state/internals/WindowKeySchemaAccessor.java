package org.apache.kafka.streams.state.internals;

public class WindowKeySchemaAccessor {
    public static long getTimestamp(byte[] key){
        return WindowKeySchema.extractStoreTimestamp(key);
    }

    public static byte[] getKeyBytes(byte[] key){
        return WindowKeySchema.extractStoreKeyBytes(key);
    }
}
