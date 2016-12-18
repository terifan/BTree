package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.Arrays;


class IndexElement
{
	final static int HEADER_SIZE = 1 + 5;

	byte [] mKey;
	long mPageIndex;


	public IndexElement()
	{
		mPageIndex = -1;
	}


	public IndexElement(byte [] aKey, long aPageIndex)
	{
		mKey = aKey;
		mPageIndex = aPageIndex;
	}


	public int size()
	{
		return HEADER_SIZE + mKey.length;
	}


	public void write(ByteBuffer aByteBuffer) throws IOException
	{
		aByteBuffer.put(mKey.length-1);
		aByteBuffer.put(mKey);
		aByteBuffer.putNumber(mPageIndex, 5);
	}


	public void read(ByteBuffer aByteBuffer) throws IOException
	{
		mKey = new byte[aByteBuffer.getUnsignedByte()+1];
		aByteBuffer.get(mKey);
		mPageIndex = aByteBuffer.getNumber(5);
	}


	@Override
	public String toString()
	{
		return Hex.encode(mKey);
	}


	@Override
	public boolean equals(Object aObject)
	{
		return BTree.compare(((IndexElement)aObject).mKey, mKey) == 0;
	}


	@Override
	public int hashCode()
	{
		return Arrays.hashCode(mKey);
	}
}