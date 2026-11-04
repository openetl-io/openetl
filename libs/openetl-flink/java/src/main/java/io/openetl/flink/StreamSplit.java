package io.openetl.flink;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** One stream; its state is the checkpoint it was taken at. The runner keeps the resume point. */
final class StreamSplit implements SourceSplit {
    final String stream;
    final long checkpoint;

    StreamSplit(String stream, long checkpoint) {
        this.stream = stream;
        this.checkpoint = checkpoint;
    }

    @Override
    public String splitId() {
        return stream;
    }

    @Override
    public String toString() {
        return stream + "@" + checkpoint;
    }

    static final class Serializer implements SimpleVersionedSerializer<StreamSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(StreamSplit s) throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(b);
            out.writeUTF(s.stream);
            out.writeLong(s.checkpoint);
            return b.toByteArray();
        }

        @Override
        public StreamSplit deserialize(int version, byte[] bytes) throws IOException {
            if (version != 1) {
                throw new IOException("openetl: a split of version " + version);
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            return new StreamSplit(in.readUTF(), in.readLong());
        }
    }
}
