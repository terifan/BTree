package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;


public class DataElementIterator implements Iterator<DataElement>, Iterable<DataElement>
{
	private long mNextDataPageIndex;
	private ArrayList<DataElement> mDataElements;
	private int mNextDataElement;
	private BTree mTree;
	private long mFailFastCounter;
	private byte [] mPrefix;
	private boolean mIsEOF;


	DataElementIterator(BTree aTree, long aPageIndex)
	{
		mTree = aTree;
		mNextDataPageIndex = aPageIndex;
		mFailFastCounter = aTree.getFailFastCounter();
	}


	DataElementIterator(BTree aTree, long aPageIndex, byte [] aPrefix)
	{
		mTree = aTree;
		mNextDataPageIndex = aPageIndex;
		mFailFastCounter = aTree.getFailFastCounter();
		mPrefix = aPrefix;

		try
		{
			Page page = mTree.loadPage(null, mNextDataPageIndex);

			if (!(page instanceof DataPage))
			{
				throw new TreeIntegrityException("Not a data page: page index: " + mNextDataPageIndex);
			}

			mNextDataPageIndex = ((DataPage)page).getNextDataPageIndex();
			mNextDataElement = -1;
			mDataElements = ((DataPage)page).getElements();

			for (int i = mDataElements.size(); --i >= 0;)
			{
				//System.out.println(new String(mPrefix)+", "+new String(mDataElements.get(i).mKey));

				if (BTree.compare(mPrefix, mDataElements.get(i).mKey) >= 0)
				{
					mNextDataElement = i;
					break;
				}
			}

			if (mNextDataElement == -1)
			{
				mIsEOF = true;
			}
		}
		catch (TreeIntegrityException | IOException e)
		{
			throw new RuntimeException(e);
		}
	}


	@Override
	public boolean hasNext()
	{
		if (mFailFastCounter != mTree.getFailFastCounter())
		{
			throw new ConcurrentModificationException("Tree has been modified.");
		}
		if (mTree.getTransactionLog() == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		if (mIsEOF)
		{
			return false;
		}
		if (mDataElements != null && mNextDataElement < mDataElements.size())
		{
			return true;
		}
		if (mDataElements != null && mNextDataElement == mDataElements.size() && mNextDataPageIndex == 0)
		{
			mIsEOF = true;
			return false;
		}

		try
		{
			Page page = mTree.loadPage(null, mNextDataPageIndex);

			if (!(page instanceof DataPage))
			{
				throw new TreeIntegrityException("Not a data page: page index: " + mNextDataPageIndex);
			}

			mNextDataPageIndex = ((DataPage)page).getNextDataPageIndex();
			mNextDataElement = 0;
			mDataElements = ((DataPage)page).getElements();
		}
		catch (TreeIntegrityException | IOException e)
		{
			throw new RuntimeException(e);
		}

		return !mDataElements.isEmpty();
	}


	@Override
	public DataElement next()
	{
		if (!hasNext())
		{
			return null;
		}
		return mDataElements.get(mNextDataElement++);
	}


	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}


	@Override
	public Iterator<DataElement> iterator()
	{
		return this;
	}
}