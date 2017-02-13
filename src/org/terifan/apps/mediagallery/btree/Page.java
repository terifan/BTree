package org.terifan.apps.mediagallery.btree;

import java.io.IOException;


abstract class Page
{
	protected IndexPage mParent;
	protected long mPageIndex;
	protected boolean mTerminated;
	protected BTree mTree;


	public abstract void write() throws IOException, TreeIntegrityException;


	public abstract void read() throws IOException, TreeIntegrityException;


	public abstract void put(DataElement aDataElement) throws IOException, TreeIntegrityException;


	public abstract int remove(IndexPage aParent, DataElement aDataElement) throws IOException, TreeIntegrityException;


	public abstract boolean get(DataElement aDataElement) throws IOException, TreeIntegrityException;


	Page(BTree aTree)
	{
		mTree = aTree;
	}


	public IndexPage getParent()
	{
		return mParent;
	}


	public long getPageIndex()
	{
		if (mPageIndex == -1)
		{
			throw new IllegalStateException("Page has no index.");
		}
		return mPageIndex;
	}


	void setPageIndex(long aPageIndex)
	{
		mPageIndex = aPageIndex;
	}
}
