package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.TreeMap;


class TransactionLog
{
	private final static boolean DEBUG = false;

	private TreeMap<Long,byte[]> mPages;
	private PageStore mPageStore;
	private boolean mStarted;
	private BTree mTree;
	private Cache<Long,byte[]> mCachedClusters;


	public TransactionLog(BTree aTree, PageStore aPageStore) throws IOException
	{
		mTree = aTree;
		mPages = new TreeMap<>();
 		mCachedClusters = new Cache<>(100);
		mPageStore = aPageStore;
/*
		mCachedClusters.addCacheStateListener(new CacheStateListener() {
			public void entryAdded(Cache aCache, Object aKey, Object aValue)
			{
				System.out.println("added   "+aKey);
			}
			public void entryUpdated(Cache aCache, Object aKey, Object aValue)
			{
				System.out.println("updated "+aKey);
			}
			public void entryRemoved(Cache aCache, Object aKey, Object aValue)
			{
				System.out.println("removed "+aKey);
			}
			public void entryDropped(Cache aCache, Object aKey, Object aValue)
			{
				System.out.println("dropped "+aKey);
			}
		});
*/
	}


	public void close() throws IOException
	{
		try
		{
			mPages.clear();
			mTree = null;
			mPages = null;
			mPageStore.close();

			if (mStarted)
			{
				throw new IllegalStateException("Changes not commited - aborting...");
			}
		}
		finally
		{
			mPageStore = null;
		}
	}


	public void start()
	{
		if (mStarted)
		{
			throw new IllegalStateException("Log already started.");
		}

		mStarted = true;
	}


	public boolean isStarted()
	{
		return mStarted;
	}


	public void commit() throws IOException
	{
		if (!mStarted)
		{
			throw new IllegalStateException("Log not started.");
		}

		for (Long pageIndex : mPages.keySet())
		{
			if (DEBUG) System.out.println("commit " + pageIndex);

			mPageStore.write(pageIndex, mPages.get(pageIndex));
		}

		mPages.clear();

		mStarted = false;
	}


	public void abort()
	{
		mPages.clear();

		mStarted = false;
	}


	public void write(long aIndex, byte [] aBuffer) throws IOException
	{
		if (DEBUG) System.out.println("write  " + aIndex);

		mPages.put(aIndex, aBuffer);

		mCachedClusters.put(aIndex, aBuffer.clone(), 1);
	}


	public void writeDirect(long aIndex, byte [] aBuffer, int aOffset, int aLength) throws IOException
	{
		if (mPages.size() > 0)
		{
			throw new IllegalStateException("Uncommmited data exists.");
		}
		if ((aLength % mPageStore.getPageSize()) != 0)
		{
			throw new IllegalStateException("Data must fill one or more pages completly: aLength: " + aLength);
		}

		mPageStore.write(aIndex, aBuffer, aOffset, aLength);
	}


	public void read(long aIndex, byte [] aBuffer) throws IOException
	{
		if (mPages.containsKey(aIndex))
		{
			if (DEBUG) System.out.println("cache1 " + aIndex);

			System.arraycopy(mPages.get(aIndex), 0, aBuffer, 0, mPageStore.getPageSize());
		}
		else if (mCachedClusters.containsKey(aIndex))
		{
			if (DEBUG) System.out.println("cache2 " + aIndex);

			System.arraycopy(mCachedClusters.get(aIndex), 0, aBuffer, 0, mPageStore.getPageSize());
		}
		else
		{
			if (DEBUG) System.out.println("read   " + aIndex);

			mPageStore.read(aIndex, aBuffer);

			mCachedClusters.put(aIndex, aBuffer.clone(), 1);
		}
	}


	public void readDirect(long aIndex, byte [] aBuffer, int aLength) throws IOException
	{
		mPageStore.read(aIndex, aBuffer, 0, aLength);
	}


	public long length() throws IOException
	{
		return mPageStore.getPageCount() * mPageStore.getPageSize();
	}
}