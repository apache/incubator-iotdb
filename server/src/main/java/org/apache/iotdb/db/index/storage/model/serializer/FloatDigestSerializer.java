package org.apache.iotdb.db.index.storage.model.serializer;

import java.math.BigDecimal;
import java.util.ArrayList;
import org.apache.iotdb.db.index.FloatDigest;
import org.apache.iotdb.db.index.utils.MyBytes;


public class FloatDigestSerializer extends DigestSerializer
{
	protected static FloatDigestSerializer instance = new FloatDigestSerializer();

	private FloatDigestSerializer() {

	}

	public static FloatDigestSerializer getInstance() {
		return instance;
	}

	public byte[] serialize(FloatDigest floatDigest) {
		ArrayList<byte[]> byteList = new ArrayList<>();

		byte[] aBytes = new byte[1];

		aBytes[0] = 2;
		byteList.add(aBytes);

		aBytes = super.serialize(floatDigest);
		byteList.add(aBytes);

		aBytes = MyBytes.floatToBytes(floatDigest.getMax());
		byteList.add(aBytes);

		aBytes = MyBytes.floatToBytes(floatDigest.getMin());
		byteList.add(aBytes);

		aBytes = MyBytes.longToBytes(floatDigest.getCount());
		byteList.add(aBytes);

		aBytes = MyBytes.floatToBytes(floatDigest.getAvg());
		byteList.add(aBytes);

		aBytes = MyBytes.StringToBytes(floatDigest.getSquareSum().toString());
		byteList.add(aBytes);

		return MyBytes.concatByteArrayList(byteList);
	}

	public FloatDigest deserialize(String key, long startTime, byte[] bytes) {

		FloatDigest dataDigest = super.deserialize(key, startTime, bytes);

		int position = 1 + 8 + 8 + 8;
		byte[] aBytes = MyBytes.subBytes(bytes, position, 4);
		float max = MyBytes.bytesToFloat(aBytes);

		position += 4;
		aBytes = MyBytes.subBytes(bytes, position, 4);
		float min = MyBytes.bytesToFloat(aBytes);

		position += 4;
		aBytes = MyBytes.subBytes(bytes, position, 8);
		long count = MyBytes.bytesToLong(aBytes);

		position += 8;
		aBytes = MyBytes.subBytes(bytes, position, 4);
		float avg = MyBytes.bytesToFloat(aBytes);

		position += 4;
		aBytes = MyBytes.subBytes(bytes, position, bytes.length-position);
		BigDecimal squareSum = new BigDecimal(MyBytes.bytesToString(aBytes));

		dataDigest.setMax(max);
		dataDigest.setMin(min);
		dataDigest.setCount(count);
		dataDigest.setAvg(avg);
		dataDigest.setSquareSum(squareSum);
		return dataDigest;
	}
}
