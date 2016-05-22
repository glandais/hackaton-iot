package com.capgemini.csd.hackaton.beans.mapdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializer;

import com.capgemini.csd.hackaton.beans.Value;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerValue implements GroupSerializer<Value> {

	@Override
	public void serialize(DataOutput2 out, Value value) throws IOException {
		out.writeLong(value.getSensorId());
		out.writeLong(value.getValue());
	}

	@Override
	public Value deserialize(DataInput2 in, int available) throws IOException {
		return new Value((int) in.readLong(), in.readLong());
	}

	@Override
	public int fixedSize() {
		return 16;
	}

	@Override
	public boolean isTrusted() {
		return true;
	}

	@Override
	public boolean equals(Value a1, Value a2) {
		//on java6 equals method is not thread safe
		return a1 == a2 || (a1 != null && a1.getSensorId() == a2.getSensorId() && a1.getValue() == a2.getValue());
	}

	@Override
	public int hashCode(Value uuid, int seed) {
		//on java6 uuid.hashCode is not thread safe. This is workaround
		long a = uuid.getSensorId() ^ uuid.getValue();
		return ((int) (a >> 32)) ^ (int) a;

	}

	@Override
	public int valueArraySearch(Object keys, Value key) {
		return Arrays.binarySearch(valueArrayToArray(keys), key);
	}

	@Override
	public int valueArraySearch(Object keys, Value key, Comparator comparator) {
		return Arrays.binarySearch(valueArrayToArray(keys), key, comparator);
	}

	@Override
	public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
		for (long o : (long[]) vals) {
			out.writeLong(o);
		}
	}

	@Override
	public Object valueArrayDeserialize(DataInput2 in, int size) throws IOException {
		size *= 2;
		long[] ret = new long[size];
		for (int i = 0; i < size; i++) {
			ret[i] = in.readLong();
		}
		return ret;
	}

	@Override
	public Value valueArrayGet(Object vals, int pos) {
		long[] v = (long[]) vals;
		pos *= 2;
		return new Value((int) v[pos++], v[pos]);
	}

	@Override
	public int valueArraySize(Object vals) {
		return ((long[]) vals).length / 2;
	}

	@Override
	public Object valueArrayEmpty() {
		return new long[0];
	}

	@Override
	public Object valueArrayPut(Object vals, int pos, Value newValue) {
		pos *= 2;

		long[] array = (long[]) vals;
		final long[] ret = Arrays.copyOf(array, array.length + 2);

		if (pos < array.length) {
			System.arraycopy(array, pos, ret, pos + 2, array.length - pos);
		}
		ret[pos++] = newValue.getSensorId();
		ret[pos] = newValue.getValue();
		return ret;
	}

	@Override
	public Object valueArrayUpdateVal(Object vals, int pos, Value newValue) {
		pos *= 2;
		long[] vals2 = ((long[]) vals).clone();
		vals2[pos++] = newValue.getSensorId();
		vals2[pos] = newValue.getValue();
		return vals2;
	}

	@Override
	public Object valueArrayFromArray(Object[] objects) {
		long[] ret = new long[objects.length * 2];
		int pos = 0;

		for (Object o : objects) {
			Value uuid = (Value) o;
			ret[pos++] = uuid.getSensorId();
			ret[pos++] = uuid.getValue();
		}

		return ret;
	}

	@Override
	public Object valueArrayCopyOfRange(Object vals, int from, int to) {
		return Arrays.copyOfRange((long[]) vals, from * 2, to * 2);
	}

	@Override
	public Object valueArrayDeleteValue(Object vals, int pos) {
		pos *= 2;
		long[] valsOrig = (long[]) vals;
		long[] vals2 = new long[valsOrig.length - 2];
		System.arraycopy(vals, 0, vals2, 0, pos - 2);
		System.arraycopy(vals, pos, vals2, pos - 2, vals2.length - (pos - 2));
		return vals2;
	}
	//
	//        @Override
	//        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
	//            if(comparator!=null && comparator!=Fun.COMPARATOR) {
	//                return super.getBTreeKeySerializer(comparator);
	//            }
	//            return BTreeKeySerializer.UUID;
	//        }
}
