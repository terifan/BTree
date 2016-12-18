package org.terifan.apps.mediagallery.btree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;



/**
 * A FilePageStore is a random access storage of pages, stored in a native file.
 * All pages are the same size and accessed by their index in the PageStore.
 *
 * Implementation note: the FilePageStore should always be closed in a finally
 * block. However, the object can be constructed outside a try..finally since
 * the underlying RandomAccessStream is created on the first read/write
 * operation.
 */
public class FilePageStore implements PageStore
{
	private final static boolean DEBUG = false;

	private RandomAccessFile mRandomAccessFile;
	private int mPageSize;
	private long mPagesPerVolume;
	private boolean mReadOnly;
	private final File mFile;


	/**
	 * Opens an existing PageStore file or creates a new PageStore file.
	 *
	 * @param aFile
	 *   the destination file.
	 * @param aReadOnly
	 *   true if reading only from file.
	 * @param aPageSize
	 *   the size of a page.
	 */
	public FilePageStore(File aFile, boolean aReadOnly, int aPageSize) throws IOException
	{
		mFile = aFile;
		mReadOnly = aReadOnly;
		mPageSize = aPageSize;

		mRandomAccessFile = new RandomAccessFile(mFile, mReadOnly ? "r" : "rw");
	}


	/**
	 * Reads one or more pages from the PageStore.
	 *
	 * @param aPageIndex
	 *   the first page to read.
	 * @param aBuffer
	 *   the destination buffer. Length must be a multiple of the page size.
	 */
	@Override
	public void read(long aPageIndex, byte [] aBuffer) throws IOException
	{
		read(aPageIndex, aBuffer, 0, aBuffer.length);
	}


	/**
	 * Reads one or more pages from the PageStore.
	 *
	 * @param aPageIndex
	 *   the first page to read.
	 * @param aBuffer
	 *   the destination buffer.
	 * @param aOffset
	 *   the start offset in the destination buffer.
	 * @param aLength
	 *   number of bytes to write. Must be a multiple of the page size.
	 */
	@Override
	public void read(long aPageIndex, byte [] aBuffer, int aOffset, int aLength) throws IOException
	{
		if (DEBUG) System.out.println("read(page="+aPageIndex+", buffer="+aBuffer+", offset="+aOffset+", length="+aLength+")");

		if (aPageIndex < 0)
		{
			throw new IllegalArgumentException("Index out of bounds: page index: "+aPageIndex);
		}
		if (aLength % mPageSize != 0)
		{
			throw new IOException("Input buffer has illegal size: " + aLength);
		}
		if (aPageIndex > getPageCount())
		{
			throw new IOException("Reading beyond end of file: page index: "+aPageIndex+", page count: " +  getPageCount());
		}

		synchronized (this)
		{
			mRandomAccessFile.seek(aPageIndex * mPageSize);
			mRandomAccessFile.read(aBuffer, aOffset, aLength);
		}
	}


	/**
	 * Writes one or more pages to the PageStore.
	 *
	 * @param aPageIndex
	 *   the start page index.
	 * @param aBuffer
	 *   the content to be written. Length must be a multiple of the page size.
	 */
	@Override
	public void write(long aPageIndex, byte [] aBuffer) throws IOException
	{
		write(aPageIndex, aBuffer, 0, aBuffer.length);
	}


	/**
	 * Writes one or more pages to the PageStore.
	 *
	 * @param aPageIndex
	 *   the start page index.
	 * @param aBuffer
	 *   the content to be written
	 * @param aOffset
	 *   the start offset in the buffer provided.
	 * @param aLength
	 *   number of bytes to be written. Must be a multiple of the page size.
	 */
	@Override
	public void write(long aPageIndex, byte [] aBuffer, int aOffset, int aLength) throws IOException
	{
		if (DEBUG) System.out.println("write(page="+aPageIndex+", buffer="+aBuffer+", offset="+aOffset+", length="+aLength+")");

		if (aPageIndex < 0)
		{
			throw new IllegalArgumentException("Index out of bounds: page index: "+aPageIndex);
		}
		if (aLength % mPageSize != 0)
		{
			throw new IOException("Input buffer has illegal size: " + aLength+", page-size: "+mPageSize);
		}
		if (aLength < 0 || aOffset+aLength > aBuffer.length)
		{
			throw new IllegalArgumentException("Index out of bounds: buffer offset: "+aOffset+", buffer length: "+aBuffer.length+", write: "+aLength);
		}

		synchronized (this)
		{
			mRandomAccessFile.seek(aPageIndex * mPageSize);
			mRandomAccessFile.write(aBuffer, aOffset, aLength);
		}
	}


	/**
	 * Returns number of pages in this PageStore.
	 *
	 * @return
	 *   number of pages
	 */
	@Override
	public long getPageCount() throws IOException
	{
		return mFile.length() / mPageSize;
	}


	/**
	 * Gets the size of a page.
	 *
	 * @return
	 *   the size of a page.
	 */
	@Override
	public int getPageSize()
	{
		return mPageSize;
	}


	/**
	 * Sets the size of a page.<p>
	 *
	 * @param aPageSize
	 *   the size of a page.
	 */
	@Override
	public void setPageSize(int aPageSize)
	{
		mPageSize = aPageSize;
	}


	/**
	 * Close the PageStore and clears all internal data. A PageStore must be
	 * explicitly closed.
	 */
	@Override
	public void close() throws IOException
	{
		synchronized (this)
		{
			if (mRandomAccessFile != null)
			{
				mRandomAccessFile.close();
				mRandomAccessFile = null;
			}
		}
	}


	@Override
	public void flush() throws IOException
	{
		synchronized (this)
		{
			mRandomAccessFile.getChannel().force(true);
		}
	}


	public boolean isReadOnly()
	{
		return mReadOnly;
	}
}