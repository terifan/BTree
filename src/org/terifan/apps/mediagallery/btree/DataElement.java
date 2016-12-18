package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.Arrays;


class DataElement
{
	byte [] mKey;
	byte [] mContent;


	DataElement()
	{
	}


	DataElement(byte [] aKey, byte [] aContent)
	{
		mKey = aKey;
		mContent = aContent;
	}


	int size()
	{
		return 1 + mKey.length + 2 + mContent.length;
	}


	void write(ByteBuffer aByteBuffer) throws IOException
	{
		aByteBuffer.put(mKey.length-1);
		aByteBuffer.put(mKey);
		aByteBuffer.putShort(mContent.length);
		aByteBuffer.put(mContent);
	}


	void read(ByteBuffer aByteBuffer) throws IOException
	{
		mKey = new byte[aByteBuffer.getUnsignedByte()+1];
		aByteBuffer.get(mKey);
		mContent = new byte[aByteBuffer.getUnsignedShort()];
		aByteBuffer.get(mContent);
	}


	public byte [] getKey()
	{
		return mKey;
	}


	public byte [] getContent()
	{
		return mContent;
	}


	@Override
	public String toString()
	{
		return Hex.encode(mKey);
	}


	@Override
	public boolean equals(Object aObject)
	{
		if (aObject instanceof DataElement)
		{
			return BTree.compare(((DataElement)aObject).mKey, mKey) == 0;
		}
		return false;
	}


	@Override
	public int hashCode()
	{
		return Arrays.hashCode(mKey);
	}
}