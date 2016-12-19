package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import javax.swing.tree.DefaultMutableTreeNode;


public class BTree implements Iterable<DataElement>
{
	private long mHeaderSize;

	private int mPageSize;
	private int mNextFreePageIndex;
	private long mFailFastCounter;
	private TransactionLog mTransactionLog;
	private Thread mShutdownHook;
	private int mMaxKeyLength;

	public static boolean DEBUG;


	public BTree(PageStore aPageStore) throws IOException, TreeIntegrityException
	{
		boolean create = aPageStore.getPageCount() == 0;

		mPageSize = aPageStore.getPageSize();
		mTransactionLog = new TransactionLog(this, aPageStore);
		mNextFreePageIndex = (int)((mTransactionLog.length()-mHeaderSize) / mPageSize);
		mMaxKeyLength = Math.min((mPageSize - IndexPage.HEADER_SIZE) / 2 - IndexElement.HEADER_SIZE, 256);

		if (create)
		{
			mTransactionLog.start();
			new DataPage(this, null).write();
			mTransactionLog.commit();
		}

		mShutdownHook = new Thread()
		{
			@Override
			public void run()
			{
				try
				{
					close();
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		};

		Runtime.getRuntime().addShutdownHook(mShutdownHook);
	}


	public synchronized boolean put(byte [] aKey, byte [] aContent) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		return putImpl(aKey, aContent);
	}


	public synchronized int get(byte [] aKey, byte [] aContent) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		return getImpl(aKey, aContent);
	}


	public synchronized boolean containsKey(byte [] aKey) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		return containsKeyImpl(aKey);
	}


	public synchronized boolean remove(byte [] aKey) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		return removeImpl(aKey);
	}


	public synchronized int length(byte [] aKey) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		return getImpl(aKey, null);
	}


	public synchronized void close() throws IOException
	{
		if (mTransactionLog != null)
		{
			mTransactionLog.close();
			mTransactionLog = null;
		}
	}


	public synchronized void startTransaction() throws IOException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		if (mTransactionLog.isStarted())
		{
			throw new IllegalStateException("Transaction already started.");
		}

		mTransactionLog.start();
	}


	public synchronized void commitTransaction() throws IOException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		if (!mTransactionLog.isStarted())
		{
			throw new IllegalStateException("Transaction not started.");
		}

		mTransactionLog.commit();
	}


	public synchronized void abortTransaction() throws IOException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		if (!mTransactionLog.isStarted())
		{
			throw new IllegalStateException("Transaction not started.");
		}

		mTransactionLog.abort();
	}


	public synchronized long putBlob(byte [] aContent, int aLength) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}
		if (mTransactionLog.isStarted())
		{
			throw new IllegalStateException("A transaction is started.");
		}

		long index = mNextFreePageIndex;

		mNextFreePageIndex += ((aLength + mPageSize - 1) / mPageSize);

		if (aLength >= mPageSize)
		{
			mTransactionLog.writeDirect(index, aContent, 0, aLength - (aLength % mPageSize));
		}

		if ((aLength % mPageSize) > 0)
		{
			byte [] temp = new byte[mPageSize];
			System.arraycopy(aContent, aLength - (aLength % mPageSize), temp, 0, aLength % mPageSize);

			mTransactionLog.writeDirect(mNextFreePageIndex-1, temp, 0, mPageSize);
		}

		return index;
	}


	public synchronized void getBlob(byte [] aContent, long aPageIndex, int aLength) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}
		if (mTransactionLog.isStarted())
		{
			throw new IllegalStateException("A transaction is started.");
		}

		mTransactionLog.readDirect(aPageIndex, aContent, aLength);
	}


	@Override
	public synchronized DataElementIterator iterator()
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		Page page = null;
		long index = 0;

		try
		{
			do
			{
				if (page instanceof IndexPage)
				{
					index = ((IndexPage)page).getFirstPageIndex();
				}
				page = loadPage((IndexPage)page, index);
			} while (page instanceof IndexPage);
		}
		catch (TreeIntegrityException | IOException e)
		{
			throw new RuntimeException(e);
		}

		return new DataElementIterator(this, page.getPageIndex());
	}


	public synchronized DataElementIterator iterator(byte [] aPrefix)
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		aPrefix = aPrefix.clone();

		Page page = null;
		long index = 0;

		try
		{
			for (;;)
			{
				page = loadPage((IndexPage)page, index);

				if (page instanceof IndexPage)
				{
					IndexPage indexPage = (IndexPage)page;

					ArrayList<IndexElement> elements = indexPage.getElements();

					if (compare(aPrefix, elements.get(0).mKey) < 0)
					{
						index = indexPage.getFirstPageIndex();
					}
					else
					{
						for (int i = elements.size(); --i >= 0;)
						{
							//System.out.println(new String(aPrefix)+" > "+new String(elements.get(i).mKey));

							if (compare(elements.get(i).mKey, aPrefix) <= 0)
							{
								index = elements.get(i).mPageIndex;
								break;
							}
						}
					}
				}
				else
				{
					break;
				}
			}
		}
		catch (TreeIntegrityException | IOException e)
		{
			throw new RuntimeException(e);
		}

		if (page == null)
		{
			throw new IllegalStateException("Page is null!");
		}

		return new DataElementIterator(this, page.getPageIndex(), aPrefix);
	}


	public synchronized void populateJTree(DefaultMutableTreeNode aJTreeNode) throws IOException, TreeIntegrityException
	{
		if (mTransactionLog == null)
		{
			throw new IllegalStateException("Tree is closed.");
		}

		mTransactionLog.start();
		populateJTreeImpl(null, 0, aJTreeNode);
		mTransactionLog.abort();
	}


	public int getMaxKeyLength()
	{
		return mMaxKeyLength;
	}


	private boolean putImpl(byte [] aKey, byte [] aContent) throws IOException, TreeIntegrityException
	{
		DataElement dataElement = new DataElement(aKey, aContent);

		if (dataElement.size() > mPageSize-DataPage.HEADER_SIZE)
		{
			throw new IOException("Supplied element exceeds maximum size: size: " + dataElement.size() + ", max-size: " + (mPageSize-DataPage.HEADER_SIZE));
		}
		if (aKey.length >= mMaxKeyLength)
		{
			throw new IOException("Supplied key exceeds maximum size: size: " + aKey.length + ", max-size: " + mMaxKeyLength);
		}

		boolean externalTransaction = mTransactionLog.isStarted();
		if (!externalTransaction)
		{
			mTransactionLog.start();
		}

		try
		{
			mFailFastCounter++;

			Page root = loadPage(null, 0);

			boolean state = root.remove(null, dataElement) == 1;

			dataElement.mContent = aContent;
			root.put(dataElement);

			if (!externalTransaction)
			{
				mTransactionLog.commit();
			}

			return state;
		}
		catch (Throwable e)
		{
			mTransactionLog.abort();
			throw new RuntimeException(e);
		}
	}


	private int getImpl(byte [] aKey, byte [] aContent) throws IOException, TreeIntegrityException
	{
		if (aKey.length >= mMaxKeyLength)
		{
			throw new IOException("Supplied key exceeds maximum size: size: " + aKey.length + ", max-size: " + mMaxKeyLength);
		}

		DataElement dataElement = new DataElement(aKey, null);

		if (loadPage(null, 0).get(dataElement))
		{
			if (aContent != null)
			{
				if (aContent.length < dataElement.mContent.length)
				{
					throw new IllegalArgumentException("Supplied buffer is to small: size: " + aContent.length + ", required: " + dataElement.mContent.length);
				}

				System.arraycopy(dataElement.mContent, 0, aContent, 0, dataElement.mContent.length);
			}

			return dataElement.mContent.length;
		}

		return 0;
	}


	private boolean containsKeyImpl(byte [] aKey) throws IOException, TreeIntegrityException
	{
		if (aKey.length >= mMaxKeyLength)
		{
			throw new IOException("Supplied key exceeds maximum size: size: " + aKey.length + ", max-size: " + mMaxKeyLength);
		}

		DataElement dataElement = new DataElement(aKey, null);

		return loadPage(null, 0).get(dataElement);
	}


	private boolean removeImpl(byte [] aKey) throws IOException, TreeIntegrityException
	{
		if (aKey.length >= mMaxKeyLength)
		{
			throw new IOException("Supplied key exceeds maximum size: size: " + aKey.length + ", max-size: " + mMaxKeyLength);
		}

		DataElement dataElement = new DataElement(aKey, null);

		boolean externalTransaction = mTransactionLog.isStarted();
		if (!externalTransaction)
		{
			mTransactionLog.start();
		}

		try
		{
			mFailFastCounter++;

			boolean r = loadPage(null, 0).remove(null, dataElement) == 1;

			if (!externalTransaction)
			{
				mTransactionLog.commit();
			}

			return r;
		}
		catch (Throwable e)
		{
			mTransactionLog.abort();
			throw new RuntimeException(e);
		}
	}


	Page loadPage(IndexPage aParent, long aPageIndex) throws IOException, TreeIntegrityException
	{
		ByteBuffer buffer = ByteBuffer.allocate(getPageSize());

		getTransactionLog().read(aPageIndex, buffer.array());

		switch (buffer.getUnsignedByte())
		{
			case 'I':
				return new IndexPage(this, aParent, aPageIndex, buffer);
			case 'D':
				return new DataPage(this, aParent, aPageIndex, buffer);
			case 'X':
				throw new TreeIntegrityException("Attemp to load a terminated page: page index: " + aPageIndex);
			default:
				throw new TreeIntegrityException("Bad page header: page index: " + aPageIndex);
		}
	}


	private void populateJTreeImpl(IndexPage aParent, long aIndex, DefaultMutableTreeNode aJTreeNode) throws IOException, TreeIntegrityException
	{
		Page page = loadPage(aParent, aIndex);

		if (page instanceof IndexPage)
		{
			DefaultMutableTreeNode newJTreePage = new DefaultMutableTreeNode("["+((IndexPage)page).getFirstPageIndex()+"]");
			aJTreeNode.add(newJTreePage);

			populateJTreeImpl((IndexPage)page, ((IndexPage)page).getFirstPageIndex(), newJTreePage);

			for (IndexElement element : ((IndexPage)page).getElements())
			{
				newJTreePage = new DefaultMutableTreeNode(element.toString());
				aJTreeNode.add(newJTreePage);

				populateJTreeImpl((IndexPage)page, element.mPageIndex, newJTreePage);
			}
		}
		else
		{
			for (DataElement element : ((DataPage)page).getElements())
			{
				aJTreeNode.add(new DefaultMutableTreeNode(element.toString()));
			}
		}
	}


	int getNextFreePageIndex()
	{
		return mNextFreePageIndex++;
	}


	long getFailFastCounter()
	{
		return mFailFastCounter;
	}


	TransactionLog getTransactionLog()
	{
		return mTransactionLog;
	}


	public int getPageSize()
	{
		return mPageSize;
	}


	/**
	 * Return
	 *   <li>-1 if aKey1 <  aKey2
	 *   <li> 0 if aKey1 == aKey2
	 *   <li> 1 if aKey1 >  aKey2
	 */
	final static int compare(byte [] aKey1, byte [] aKey2)
	{
		for (int i = 0, sz = Math.min(aKey1.length, aKey2.length); i < sz; i++)
		{
			int c1 = aKey1[i]&255;
			int c2 = aKey2[i]&255;
			if (c1 < c2) return -1;
			if (c1 > c2) return 1;
		}

		if (aKey1.length < aKey2.length) return -1;
		if (aKey1.length > aKey2.length) return 1;

		return 0;
	}


	final static boolean startsWith(byte [] aKey, byte [] aPattern)
	{
		if (aPattern.length > aKey.length)
		{
			return false;
		}

		for (int i = 0; i < aPattern.length; i++)
		{
			if (aPattern[i] != aKey[i])
			{
				return false;
			}
		}

		return true;
	}


	private static byte [] toByteArray(String aKey)
	{
		try
		{
			return aKey.getBytes("utf8");
		}
		catch (UnsupportedEncodingException e)
		{
			throw new RuntimeException(e);
		}
	}


	IndexPage findNearestParent(byte [] aKey) throws IOException
	{
		Page page = loadPage(null, 0);
		for (;;)
		{
			Page tmp = page;

			page = loadPage((IndexPage)page, ((IndexPage)page).findChildPageIndex(aKey));

			if (page instanceof DataPage)
			{
				return (IndexPage)tmp;
			}
		}
	}


	protected long getHeaderSize()
	{
		return mHeaderSize;
	}
}