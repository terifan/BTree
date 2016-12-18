package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.ArrayList;


class DataPage extends Page
{
	final static int HEADER_SIZE = 1+5+5+2;

	private ArrayList<DataElement> mElements;
	private long mNextDataPageIndex;
	private long mPreviousDataPageIndex;
	private int mUsedSize;


	public DataPage(BTree aTree, IndexPage aParent)
	{
		super(aTree);

		mParent = aParent;
		mElements = new ArrayList<>();
		mUsedSize = HEADER_SIZE;
		mNextDataPageIndex = 0; // 0 == NIL
		mPreviousDataPageIndex = 0; // 0 == NIL
		mPageIndex = -1;
	}


	public DataPage(BTree aTree, IndexPage aParent, long aPageIndex) throws IOException, TreeIntegrityException
	{
		this(aTree, aParent);

		mPageIndex = aPageIndex;

		read();
	}


	public DataPage(BTree aTree, IndexPage aParent, long aPageIndex, ByteBuffer aData) throws IOException, TreeIntegrityException
	{
		this(aTree, aParent);

		mPageIndex = aPageIndex;

		parseElements(aData);
	}


	@Override
	public void put(DataElement aDataElement) throws IOException, TreeIntegrityException
	{
		if (mUsedSize + aDataElement.size() > mTree.getPageSize())
		{
			boolean done = false;

			for (int i = 0; i < mElements.size(); i++)
			{
				if (BTree.compare(mElements.get(i).getKey(), aDataElement.getKey()) >= 0)
				{
					mElements.add(i, aDataElement);
					done = true;
					break;
				}
			}

			if (!done)
			{
				mElements.add(aDataElement);
			}

			mUsedSize += aDataElement.size();

			if (mParent == null) // special case when tree only contains a single data page
			{
				IndexPage newIndexPage = new IndexPage(mTree, null);

				DataPage leftDataPage = new DataPage(mTree, newIndexPage);
				DataPage rightDataPage = new DataPage(mTree, newIndexPage);

				int threshold = (mUsedSize-HEADER_SIZE) / 2;

				for (int i = 0, sz = mElements.size(), size = 0; i < sz; i++)
				{
					DataElement element = mElements.get(i);
					size += element.size();
					if (size < threshold || i == 0)
					{
						leftDataPage.put(element);
					}
					else
					{
						rightDataPage.put(element);
					}

				}

				leftDataPage.mNextDataPageIndex = rightDataPage.getPageIndex();
				rightDataPage.mPreviousDataPageIndex = leftDataPage.getPageIndex();

				leftDataPage.write();
				rightDataPage.write();

				newIndexPage.setPageIndex(0);
				newIndexPage.setFirstPageIndex(leftDataPage.getPageIndex());
				newIndexPage.getElements().add(new IndexElement(rightDataPage.mElements.get(0).getKey(), rightDataPage.getPageIndex()));
				newIndexPage.write();
			}
			else
			{
				ArrayList<ArrayList<DataElement>> lists = new ArrayList<ArrayList<DataElement>>();

				if (mElements.size() == 2)
				{
					ArrayList<DataElement> a = new ArrayList<>();
					ArrayList<DataElement> b = new ArrayList<>();
					a.add(mElements.get(0));
					b.add(mElements.get(1));
					lists.add(a);
					lists.add(b);
				}
				else
				{
					int totalSize = 0;
					int maxSize = mTree.getPageSize() - HEADER_SIZE;
					int [] sizes = new int[mElements.size()];

					for (int i = 0; i < mElements.size(); i++)
					{
						int s = mElements.get(i).size();
						sizes[i] = s;
						totalSize += s;
					}

					int [] partCount = {1,mElements.size()-2,1};
					int [] partSize = new int[3];
					partSize[0] = sizes[0];
					partSize[2] = sizes[mElements.size()-1];
					partSize[1] = totalSize-partSize[0]-partSize[2];

					while (partSize[1] > 0)
					{
						boolean moveLeft = partSize[0] + sizes[partCount[0]] <= maxSize;
						boolean moveRight = partSize[2] + sizes[partCount[0]+partCount[1]-1] <= maxSize;

						if (moveLeft && moveRight && partSize[0] > partSize[2])
						{
							moveLeft = false;
						}
						if (moveLeft && moveRight && partSize[0] < partSize[2])
						{
							moveRight = false;
						}

						if (moveLeft)
						{
							partSize[0] += sizes[partCount[0]];
							partSize[1] -= sizes[partCount[0]];
							partCount[0]++;
							partCount[1]--;
						}
						if (partCount[1] > 0 && moveRight)
						{
							partSize[2] += sizes[partCount[0]+partCount[1]-1];
							partSize[1] -= sizes[partCount[0]+partCount[1]-1];
							partCount[2]++;
							partCount[1]--;
						}
						if (!moveLeft && !moveRight)
						{
							break;
						}
					}

//System.out.println(partSize[0]+"\t"+partSize[1]+"\t"+partSize[2]+"\t"+totalSize+"\t"+(totalSize==partSize[0]+partSize[1]+partSize[2]));

					int checkSize = 0;

					for (int i = 0, j = 0; i < 3; i++)
					{
						if (partCount[i] > 0)
						{
							ArrayList<DataElement> list = new ArrayList<>();

							for (int k = 0; k < partCount[i]; k++)
							{
								DataElement e = mElements.get(j++);
								list.add(e);
								checkSize += e.size();
							}

							lists.add(list);
						}
					}

					if (totalSize != checkSize)
					{
						throw new RuntimeException();
					}
				}

/*
				int [] partSize = new int[3];
				int [] partPosition = new int[3];
				int goalSize = totalSize / 3;

				for (int i = 0, pos = 0, size = 0, partIndex = 0; i < mElements.size(); i++)
				{
					int s = mElements.get(i).size();

					size += s;
					partSize[partIndex] = size;
					partPosition[partIndex] = pos;

					if (size > goalSize && partIndex < 2)
					{
						if (Math.abs(size-s-goalSize) < Math.abs(size-goalSize))
						{
							partSize[partIndex] -= s;
						}
						i--;
						size = 0;
						pos = i;
						partIndex++;
					}
				}

System.out.println(partSize[0]+"\t"+partSize[1]+"\t"+partSize[2]+"\t"+goalSize+"\t"+totalSize+"\t"+(partSize[0]+partSize[1]+partSize[2]));
*/

/*
				{
					ArrayList<DataElement> list = new ArrayList<DataElement>();
					lists.add(list);

					int threshold = mTree.getPageSize();

					for (int i = 0, size = HEADER_SIZE; i < mElements.size(); i++)
					{
						DataElement element = mElements.get(i);

						if (size + element.size() > threshold && list.size() > 0)
						{
							list = new ArrayList<DataElement>();
							lists.add(list);
							size = HEADER_SIZE;
						}

						size += element.size();
						list.add(element);
					}
				}
*/

int ss = HEADER_SIZE;
for(ArrayList<DataElement> l : lists)
{
int sss = HEADER_SIZE;
for(DataElement e : l)
{
ss+=e.size();
sss+=e.size();
}
if(sss>mTree.getPageSize()) throw new RuntimeException("sss>mTree.getPageSize(): "+sss);
}
if(ss!=mUsedSize) throw new RuntimeException("ss!=mUsedSize: "+ss);


				long rightLink = mNextDataPageIndex;

				ArrayList<DataPage> pages = new ArrayList<>();
				pages.add(this);

				DataPage dataPage = null;

				for (int i = 0; i < lists.size(); i++)
				{
					ArrayList<DataElement> elements = lists.get(i);

					DataPage prevDataPage = dataPage;

					if (i == 0)
					{
						dataPage = this;
					}
					else
					{
						dataPage = new DataPage(mTree, mParent);
						pages.add(dataPage);
					}

					dataPage.mElements = elements;
					dataPage.mUsedSize = HEADER_SIZE;
					for (DataElement element : elements)
					{
						dataPage.mUsedSize += element.size();
					}

					if (prevDataPage != null)
					{
						dataPage.mPreviousDataPageIndex = prevDataPage.getPageIndex();
					}

					dataPage.write();

					if (prevDataPage != null)
					{
						prevDataPage.mNextDataPageIndex = dataPage.getPageIndex();
						prevDataPage.write();
					}
				}

				dataPage.mNextDataPageIndex = rightLink;
				dataPage.write();

				if (dataPage.mNextDataPageIndex != 0)
				{
					DataPage page = (DataPage)mTree.loadPage(null, dataPage.mNextDataPageIndex);
					page.mPreviousDataPageIndex = dataPage.getPageIndex();
					page.write();
				}

				for (int i = 1; i < pages.size(); i++)
				{
					byte [] key = pages.get(i).mElements.get(0).getKey();
					IndexPage parent = mTree.findNearestParent(key);
					mParent.insertElement(new IndexElement(key, pages.get(i).getPageIndex()));
				}
			}

			return;
		}

		mUsedSize += aDataElement.size();

		for (int i = 0; i < mElements.size(); i++)
		{
			if (BTree.compare(mElements.get(i).getKey(), aDataElement.getKey()) >= 0)
			{
				mElements.add(i, aDataElement);
				write();
				return;
			}
		}

		mElements.add(aDataElement);
		write();
	}


	@Override
	public int remove(IndexPage aParent, DataElement aDataElement) throws IOException, TreeIntegrityException
	{
		if (!get(aDataElement))
		{
			return 0;
		}

		mElements.remove(aDataElement);
		mUsedSize -= aDataElement.size();

		if (!mElements.isEmpty())
		{
			write();
			return 1;
		}

		// Page is empty and will be terminated by the parent page.

		write();

		if (mPreviousDataPageIndex != 0)
		{
			DataPage page = (DataPage)mTree.loadPage(null, mPreviousDataPageIndex);
			page.mNextDataPageIndex = mNextDataPageIndex;
			page.write();
		}

		if (mNextDataPageIndex != 0)
		{
			DataPage page = (DataPage)mTree.loadPage(null, mNextDataPageIndex);
			page.mPreviousDataPageIndex = mPreviousDataPageIndex;
			page.write();
		}

		return 2;
	}


	@Override
	public boolean get(DataElement aDataElement) throws IOException, TreeIntegrityException
	{
		for (int i = 0; i < mElements.size(); i++)
		{
			if (BTree.compare(aDataElement.mKey, mElements.get(i).mKey) == 0)
			{
				aDataElement.mContent = mElements.get(i).mContent;
				return true;
			}
		}

		return false;
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
			buffer.put('D');
			buffer.putNumber(mPreviousDataPageIndex, 5);
			buffer.putNumber(mNextDataPageIndex, 5);
			buffer.putShort(mElements.size());

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
		mElements.clear();

		ByteBuffer buffer = ByteBuffer.allocate(mTree.getPageSize());

		mTree.getTransactionLog().read(mPageIndex, buffer.array());

		if (buffer.getUnsignedByte() != 'D')
		{
			throw new TreeIntegrityException("Not a data page: page index: " + mPageIndex);
		}

		parseElements(buffer);
	}


	private void parseElements(ByteBuffer aBuffer) throws IOException, TreeIntegrityException
	{
		mPreviousDataPageIndex = aBuffer.getNumber(5);
		mNextDataPageIndex = aBuffer.getNumber(5);
		mUsedSize = HEADER_SIZE;

		for (int i = aBuffer.getUnsignedShort(); --i >= 0;)
		{
			DataElement element = new DataElement();
			element.read(aBuffer);
			mElements.add(element);
			mUsedSize += element.size();
		}
	}


	ArrayList<DataElement> getElements()
	{
		return mElements;
	}


	long getNextDataPageIndex()
	{
		return mNextDataPageIndex;
	}


	long getPreviousDataPageIndex()
	{
		return mPreviousDataPageIndex;
	}
}