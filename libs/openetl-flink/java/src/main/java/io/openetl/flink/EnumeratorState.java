package io.openetl.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/** The checkpoint the enumerator's state was taken at, and the streams it had assigned. */
final class EnumeratorState {
    final long checkpoint;
    final Set<String> assigned;

    EnumeratorState(long checkpoint, Set<String> assigned) {
        this.checkpoint = checkpoint;
        this.assigned = new TreeSet<>(assigned);
    }

    static final class Serializer implements SimpleVersionedSerializer<EnumeratorState> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(EnumeratorState s) throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(b);
            out.writeLong(s.checkpoint);
            out.writeInt(s.assigned.size());
            for (String a : s.assigned) {
                out.writeUTF(a);
            }
            return b.toByteArray();
        }

        @Override
        public EnumeratorState deserialize(int version, byte[] bytes) throws IOException {
            if (version != 1) {
                throw new IOException("openetl: enumerator state of version " + version);
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            long c = in.readLong();
            int n = in.readInt();
            Set<String> a = new TreeSet<>();
            for (int i = 0; i < n; i++) {
                a.add(in.readUTF());
            }
            return new EnumeratorState(c, a);
        }
    }
}
