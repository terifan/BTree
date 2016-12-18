package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.ArrayList;


class IndexPage extends Page
{
	final static int HEADER_SIZE = 1+2+5;
	final static int FIRST_CHILD = 1<<31;

	private ArrayList<IndexElement> mElements;
	private long mFirstPageIndex;


	public IndexPage(BTree aTree, IndexPage aParent)
	{
		super(aTree);

		mParent = aParent;
		mElements = new ArrayList<>();
		mFirstPageIndex = -1;
		mPageIndex = -1;
	}


	public IndexPage(BTree aTree, IndexPage aParent, long aPageIndex) throws IOException, TreeIntegrityException
	{
		this(aTree, aParent);

		mPageIndex = aPageIndex;

		read();
	}


	public IndexPage(BTree aTree, IndexPage aParent, long aPageIndex, ByteBuffer aData) throws IOException, TreeIntegrityException
	{
		this(aTree, aParent);

		mPageIndex = aPageIndex;

		parseElements(aData);
	}


	@Override
	public void put(DataElement aDataElement) throws IOException, TreeIntegrityException
	{
		if (mElements.isEmpty() || BTree.compare(aDataElement.getKey(), mElements.get(0).mKey) == -1)
		{
			mTree.loadPage(this, mFirstPageIndex).put(aDataElement);
			write();
			return;
		}

		for (int i = mElements.size(); --i >= 0;)
		{
			IndexElement element = mElements.get(i);
			if (BTree.compare(aDataElement.getKey(), element.mKey) >= 0)
			{
				mTree.loadPage(this, element.mPageIndex).put(aDataElement);
				write();
				return;
			}
		}

		throw new RuntimeException();
	}


	@Override
	public boolean get(DataElement aDataElement) throws IOException, TreeIntegrityException
	{
		if (mElements.isEmpty() || BTree.compare(aDataElement.getKey(), mElements.get(0).mKey) == -1)
		{
			return mTree.loadPage(this, mFirstPageIndex).get(aDataElement);
		}

		for (int i = mElements.size(); --i >= 0;)
		{
			IndexElement element = mElements.get(i);
			if (BTree.compare(aDataElement.getKey(), element.mKey) >= 0)
			{
				return mTree.loadPage(this, element.mPageIndex).get(aDataElement);
			}
		}

		return false;
	}


	@Override
	public int remove(IndexPage aParent, DataElement aDataElement) throws IOException, TreeIntegrityException
	{
		int state = -1;
		int index = -1;

		Page childPage = null;

		if (mElements.isEmpty() || BTree.compare(aDataElement.getKey(), mElements.get(0).mKey) == -1)
		{
			childPage = mTree.loadPage(this, mFirstPageIndex);
			state = childPage.remove(this, aDataElement);
		}

		if (state == -1)
		{
			for (int i = mElements.size(); --i >= 0;)
			{
				IndexElement element = mElements.get(i);
				if (BTree.compare(aDataElement.getKey(), element.mKey) >= 0)
				{
					childPage = mTree.loadPage(this, element.mPageIndex);
					state = childPage.remove(this, aDataElement);
					index = i;
					break;
				}
			}
		}

if(BTree.DEBUG)
{
	System.out.println("state="+state);
}

		if (state < 2) // state 0 = not found, state 1 = element deleted, done!
		{
			return state;
		}

		// state 2 occurs when a data page is empty and it has terminated itself
		// state 3 occurs when two index pages has merged and their parent may need to be updated

		if (state == 2)
		{
			if (mPageIndex == 0 && mElements.isEmpty())
			{
				return 1;
			}

			childPage.mTerminated = true;
			childPage.write();

			if (index == -1)
			{
				mFirstPageIndex = mElements.remove(0).mPageIndex;
			}
			else
			{
				mElements.remove(index);
			}
			write();
		}

		if (getUsedSpace() > mTree.getPageSize()/2 || mPageIndex == 0 && mElements.size() > 0) // not underflow
		{
			return 1;
		}

		if (mPageIndex == 0) // collapse root (root has only one child)
		{
			Page n = mTree.loadPage(this, mFirstPageIndex);

			if (n instanceof IndexPage)
			{
				IndexPage page = (IndexPage)n;

				mFirstPageIndex = page.mFirstPageIndex;

				for (int i = 0; i < page.mElements.size(); i++)
				{
					mElements.add(page.mElements.get(i));
				}

				page.mTerminated = true;
				page.write();

				write();
			}

			return 1;
		}

		int neighborIndexInParent = 1<<31;
		IndexPage neighborPage = null;
		boolean leftSideNeighbor = true;
		boolean shiftElements = false;
		boolean mergeElements = false;

		int size = getUsedSpace();
		boolean done = false;

/*		if (mPageIndex == aParent.mFirstPageIndex)
		{
			neighborPage = (IndexPage)mTree.loadPage(aParent, aParent.mElements.get(0).mPageIndex);
			neighborIndexInParent = 0;
			leftSideNeighbor = false;

			int smallestKeySize = IndexElement.HEADER_SIZE + findSmallestKey(neighborPage).length;
			int neighborSize = neighborPage.getUsedSpace() + smallestKeySize - HEADER_SIZE;

			if (neighborPage.mElements.size() > 1 && neighborSize < mTree.getPageSize() / 2 && size + smallestKeySize <= mTree.getPageSize())
			{
				done = true;
				shiftElements = true;
			}
			else if (size + neighborSize <= mTree.getPageSize())
			{
				done = true;
				mergeElements = true;
			}
		}
		else if (mPageIndex == aParent.mElements.get(0).mPageIndex)
		{
			neighborPage = (IndexPage)mTree.loadPage(aParent, aParent.mFirstPageIndex);
			neighborIndexInParent = FIRST_CHILD;
			leftSideNeighbor = true;

			int smallestKeySize = IndexElement.HEADER_SIZE + findSmallestKey(this).length;
			int neighborSize = neighborPage.getUsedSpace();

			if (mElements.size() > 1 && neighborSize < mTree.getPageSize() / 2 && size + smallestKeySize <= mTree.getPageSize())
			{
				done = true;
				shiftElements = true;
			}
			else if (size + neighborSize <= mTree.getPageSize())
			{
				done = true;
				mergeElements = true;
			}
		}*/



		if (mPageIndex == aParent.mFirstPageIndex)
		{
			neighborPage = (IndexPage)mTree.loadPage(aParent, aParent.mElements.get(0).mPageIndex);
			neighborIndexInParent = 0;
			leftSideNeighbor = false;
		}
		else if (mPageIndex == aParent.mElements.get(0).mPageIndex)
		{
			neighborPage = (IndexPage)mTree.loadPage(aParent, aParent.mFirstPageIndex);
			neighborIndexInParent = FIRST_CHILD;
		}
		else
		{
			for (int i = 1; i < aParent.mElements.size(); i++)
			{
				IndexElement element = aParent.mElements.get(i);

				if (element.mPageIndex == mPageIndex)
				{
					neighborPage = (IndexPage)mTree.loadPage(aParent, aParent.mElements.get(i-1).mPageIndex);
					neighborIndexInParent = i-1;
					break;
				}
			}

			if (neighborPage == null)
			{
				throw new RuntimeException("neighbor == null");
			}
		}

if(BTree.DEBUG)
{
	throw new RuntimeException("debug"+mPageIndex+", "+neighborPage.getUsedSpace());
}

		if (neighborPage.mElements.size() > 1 && neighborPage.getUsedSpace() > mTree.getPageSize()/2) // shift elements from neighbor
		{
			if (leftSideNeighbor)
			{
				byte [] key = findSmallestKey(this);

				mElements.add(0, new IndexElement(key, mFirstPageIndex));

				int middle = -1;
				int leftSize = neighborPage.getUsedSpace();
				int rightSize = getUsedSpace();
				int diff = 1<<30;

				for (int i = neighborPage.mElements.size(); --i >= 1;)
				{
					int es = neighborPage.mElements.get(i).size();
					leftSize -= es;
					rightSize += es;
					int d = Math.abs(leftSize-rightSize);

					if (diff > d && leftSize <= mTree.getPageSize() && rightSize <= mTree.getPageSize())
					{
						diff = d;
						middle = i;
					}
				}

				mFirstPageIndex = neighborPage.mElements.remove(middle).mPageIndex;

				write();

				for (int i = neighborPage.mElements.size(); --i >= middle;)
				{
					if (insertElement(neighborPage.mElements.remove(i)))
					{
						throw new RuntimeException();
					}
				}

				neighborPage.write();

				key = findSmallestKey(this);

				int i = aParent.indexOf(mPageIndex);
				if (i == -1)
				{
					throw new RuntimeException();
				}
				aParent.mElements.remove(i);
				aParent.write();
				aParent.insertElement(new IndexElement(key, mPageIndex));

/*				boolean b = true;
				for (int i = 0; i < aParent.mElements.size(); i++)
				{
					if (aParent.mElements.get(i).mPageIndex == mPageIndex)
					{
						b = false;
						aParent.mElements.remove(i);
						aParent.write();
						break;
					}
				}
				if(b)throw new RuntimeException();*/
			}
			else
			{
				byte [] key = findSmallestKey(neighborPage);

				mElements.add(new IndexElement(key, neighborPage.mFirstPageIndex));

				int middle = -1;
				int leftSize = getUsedSpace();
				int rightSize = neighborPage.getUsedSpace();
				int diff = 1<<30;

				for (int i = 0; i < neighborPage.mElements.size()-1; i++)
				{
					int es = neighborPage.mElements.get(i).size();
					leftSize += es;
					rightSize -= es;
					int d = Math.abs(leftSize-rightSize);
					if (diff > d && leftSize <= mTree.getPageSize() && rightSize <= mTree.getPageSize())
					{
						diff = d;
						middle = i;
					}
				}

				neighborPage.mFirstPageIndex = neighborPage.mElements.remove(middle).mPageIndex;

				neighborPage.write();
				write();

				for (int i = middle; --i >= 0;)
				{
					if (insertElement(neighborPage.mElements.remove(i)))
					{
						throw new RuntimeException();
					}
				}

				neighborPage.write();
				write();

				key = findSmallestKey(neighborPage);

				int i = aParent.indexOf(neighborPage.mPageIndex);
				if (i == -1)
				{
					throw new RuntimeException();
				}

				aParent.mElements.remove(i);
				aParent.write();
				aParent.insertElement(new IndexElement(key, neighborPage.mPageIndex));

/*				boolean b = true;
				for (int i = 0; i < aParent.mElements.size(); i++)
				{
					if (aParent.mElements.get(i).mPageIndex == neighborPage.mPageIndex)
					{
						b = false;
						aParent.mElements.remove(i);
						aParent.write();
						break;
					}
				}
				if(b)throw new RuntimeException();*/
			}

			return 1;
		}

		// merge with neighbor

		if (leftSideNeighbor)
		{
			if (neighborIndexInParent == FIRST_CHILD)
			{
				aParent.mElements.remove(0);
			}
			else
			{
				aParent.mElements.remove(neighborIndexInParent+1);
			}
			aParent.write();

			byte [] key = findSmallestKey(this);

			if (neighborPage.insertElement(new IndexElement(key, mFirstPageIndex)))
			{
				throw new RuntimeException();
			}

			for (int i = 0; i < mElements.size(); i++)
			{
				if (neighborPage.insertElement(mElements.get(i)))
				{
					throw new RuntimeException();
				}
			}

			mTerminated = true;
			write();
		}
		else
		{
			if (neighborIndexInParent == FIRST_CHILD) // TODO: G�R V�L ALDRIG IN H�R?
			{
				aParent.mFirstPageIndex = aParent.mElements.remove(0).mPageIndex;
			}
			else
			{
				aParent.mElements.remove(neighborIndexInParent);
			}
			aParent.write();

			byte [] key = findSmallestKey(neighborPage);

			if (insertElement(new IndexElement(key, neighborPage.mFirstPageIndex)))
			{
				throw new RuntimeException();
			}

			for (int i = 0; i < neighborPage.mElements.size(); i++)
			{
				if (insertElement(neighborPage.mElements.get(i)))
				{
					throw new RuntimeException();
				}
			}

			neighborPage.mTerminated = true;
			neighborPage.write();
		}

		return 3;
	}


	/**
	 * @return true if this insert caused the page to split
	 */
	public boolean insertElement(IndexElement aIndexElement) throws IOException, TreeIntegrityException
	{
		if (getUsedSpace() + aIndexElement.size() > mTree.getPageSize())
		{
			if (BTree.compare(mElements.get(0).mKey, aIndexElement.mKey) == 1)
			{
				mElements.add(0, aIndexElement);
			}
			else
			{
				for (int i = mElements.size(); --i >= 0;)
				{
					IndexElement element = mElements.get(i);
					if (BTree.compare(element.mKey,  aIndexElement.mKey) <= 0)
					{
						mElements.add(i+1, aIndexElement);
						break;
					}
				}
			}

			int middle = 0;

			{
				int threshold = (getUsedSpace()-HEADER_SIZE) / 2;

				for (int sz = mElements.size()-2, size = 0; middle < sz; middle++)
				{
					if (size >= threshold)
					{
						break;
					}

					size += mElements.get(middle).size();
				}
			}

if (middle == 0)
{
	throw new RuntimeException("middle == 0");
}

			if (getPageIndex() == 0) // split root page
			{
				IndexPage leftPage = new IndexPage(mTree, this);
				IndexPage rightPage = new IndexPage(mTree, this);

				byte [] rightKey = mElements.get(middle).mKey;

				leftPage.mFirstPageIndex = mFirstPageIndex;
				rightPage.mFirstPageIndex = mElements.get(middle).mPageIndex;

				for (int i = middle; --i >= 0;)
				{
					if (leftPage.insertElement(mElements.get(i)))
					{
						throw new RuntimeException();
					}
				}

				for (int i = mElements.size(); --i > middle;)
				{
					if (rightPage.insertElement(mElements.get(i)))
					{
						throw new RuntimeException();
					}
				}

				mElements.clear();

				mFirstPageIndex = leftPage.getPageIndex();
				insertElement(new IndexElement(rightKey, rightPage.getPageIndex()));

				return true;
			}
			else // split internal index page
			{
				IndexPage newPage = new IndexPage(mTree, this);

				byte [] middleKey = mElements.get(middle).mKey;

				newPage.mFirstPageIndex = mElements.get(middle).mPageIndex;

				for (int i = mElements.size(); --i > middle;)
				{
					IndexElement element = mElements.get(i);
					if (newPage.insertElement(element))
					{
						throw new RuntimeException();
					}
					mElements.remove(i);
				}

				mElements.remove(middle);

				write();

				mParent.insertElement(new IndexElement(middleKey, newPage.getPageIndex()));

				return true;
			}
		}

		if (mElements.size() == 0)
		{
			mElements.add(aIndexElement);
			write();
			return false;
		}

		if (BTree.compare(mElements.get(0).mKey, aIndexElement.mKey) == 1)
		{
			mElements.add(0, aIndexElement);
			write();
			return false;
		}

		for (int i = mElements.size(); --i >= 0;)
		{
			IndexElement element = mElements.get(i);
			if (BTree.compare(element.mKey, aIndexElement.mKey) <= 0)
			{
				mElements.add(i+1, aIndexElement);
				write();
				return false;
			}
		}

		throw new RuntimeException(""+aIndexElement);
	}


	@Override
	public void write() throws IOException, TreeIntegrityException
	{
		if (mPageIndex == -1)
		{
			mPageIndex = mTree.getNextFreePageIndex();
		}

		ByteBuffer buffer = ByteBuffer.allocate(mTree.getPageSize());

		if (mTerminated)
		{
			buffer.put('X');
		}
		else
		{
			buffer.put('I');
			buffer.putShort(mElements.size());
			buffer.putNumber(mFirstPageIndex, 5);

			for (int i = 0; i < mElements.size(); i++)
			{
				mElements.get(i).write(buffer);
			}
		}

		mTree.getTransactionLog().write(mPageIndex, buffer.array());
	}


	@Override
	public void read() throws IOException, TreeIntegrityException
	{
		ByteBuffer buffer = ByteBuffer.allocate(mTree.getPageSize());

		mTree.getTransactionLog().read(mPageIndex, buffer.array());

		if (buffer.getUnsignedByte() != 'I')
		{
			throw new IOException("Not an index page: page index: " + mPageIndex);
		}

		parseElements(buffer);
	}


	private void parseElements(ByteBuffer aBuffer) throws IOException, TreeIntegrityException
	{
		mElements.clear();

		int l = aBuffer.getUnsignedShort();
		mFirstPageIndex = aBuffer.getNumber(5);

		for (int i = 0; i < l; i++)
		{
			IndexElement element = new IndexElement();
			element.read(aBuffer);
			mElements.add(element);
		}
	}


	ArrayList<IndexElement> getElements()
	{
		return mElements;
	}


	long getFirstPageIndex()
	{
		return mFirstPageIndex;
	}


	void setFirstPageIndex(long aFirstPageIndex)
	{
		mFirstPageIndex = aFirstPageIndex;
	}


	int getUsedSpace()
	{
		int sz = HEADER_SIZE;
		for (int i = mElements.size(); --i >= 0;)
		{
			sz += mElements.get(i).size();
		}
		return sz;
	}


	long findChildPageIndex(byte [] aKey) throws IOException
	{
		if (mElements.size() == 0 || BTree.compare(aKey, mElements.get(0).mKey) == -1)
		{
			return mFirstPageIndex;
		}

		for (int i = mElements.size(); --i >= 0;)
		{
			IndexElement element = mElements.get(i);
			if (BTree.compare(aKey, element.mKey) >= 0)
			{
				return element.mPageIndex;
			}
		}

		throw new IOException("error");
	}


	private byte [] findSmallestKey(Page aPage) throws IOException
	{
		Page page = aPage;

		do
		{
			page = mTree.loadPage((IndexPage)page, ((IndexPage)page).mFirstPageIndex);
		}
		while (page instanceof IndexPage);

		return ((DataPage)page).getElements().get(0).getKey();
	}


	private int indexOf(long aPageIndex)
	{
		if (mFirstPageIndex == aPageIndex)
		{
			return FIRST_CHILD;
		}
		for (int i = mElements.size(); --i >= 0;)
		{
			if (mElements.get(i).mPageIndex == aPageIndex)
			{
				return i;
			}
		}
		return -1;
	}
}