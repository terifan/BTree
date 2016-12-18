package org.terifan.apps.mediagallery.btree;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;


public class TreeValidator
{
	private String mIntegrityCheckMessage;
	private BitSet usage = new BitSet();
	private BitSet refs = new BitSet();
	private BitSet linkRefPrev = new BitSet();
	private BitSet linkRefNext = new BitSet();
	private HashSet<byte[]> keys = new HashSet<>();
	private BTree mTree;


	public TreeValidator(BTree aTree)
	{
		mTree = aTree;
	}


	public synchronized boolean integrityCheck() throws IOException, TreeIntegrityException
	{
		mIntegrityCheckMessage = "OK";
		usage = new BitSet();
		refs = new BitSet();
		linkRefPrev = new BitSet();
		linkRefNext = new BitSet();
		keys = new HashSet<>();

		if (!integrityCheck(null, 0, usage, refs, keys))
		{
			return false;
		}

		for (int i = 0; i < usage.length(); i++)
		{
			boolean terminated = false;
			try
			{
				mTree.loadPage(null, i);
			}
			catch (TreeIntegrityException e)
			{
				if (!e.getMessage().startsWith("Attemp to load a terminated page"))
				{
					mIntegrityCheckMessage = e.getMessage();
					return false;
				}
				terminated = true;
			}

			if (terminated && usage.get(i))
			{
				mIntegrityCheckMessage = "Terminated page used: page index: "+i;
				return false;
			}
			else if (!terminated && !usage.get(i))
			{
				mIntegrityCheckMessage = "Page never used: page index: "+i;
				return false;
			}

			if (!terminated && !refs.get(i) && i != 0)
			{
				mIntegrityCheckMessage = "Page never referenced: page index: "+i;
				return false;
			}
			else if (terminated && refs.get(i))
			{
				mIntegrityCheckMessage = "Terminated page referenced: page index: "+i;
				return false;
			}
		}

		try
		{
			integrityCheckOrder(mTree.loadPage(null,0), new byte[0], linkRefPrev, linkRefNext);
		}
		catch (IOException e)
		{
			mIntegrityCheckMessage = e.getMessage();
		}

//		checkIndexNodeTypes();

		return mIntegrityCheckMessage.equals("OK");
	}


	public String getIntegrityCheckMessage()
	{
		return mIntegrityCheckMessage;
	}


	private byte [] integrityCheckOrder(Page aParent, byte [] aPrevKey, BitSet aLinkRefPrev, BitSet aLinkRefNext) throws IOException, TreeIntegrityException
	{
		if (aParent instanceof DataPage)
		{
			DataPage parent = (DataPage)aParent;

			if (aLinkRefPrev.get((int)parent.getPreviousDataPageIndex()) && parent.getPreviousDataPageIndex() != 0)
			{
				throw new IOException("Data page already referenced in a link: page index: " + parent.getPageIndex()+", previous points to page index: "+parent.getPreviousDataPageIndex());
			}
			aLinkRefPrev.set((int)parent.getPreviousDataPageIndex(), true);

			if (aLinkRefNext.get((int)parent.getNextDataPageIndex()) && parent.getNextDataPageIndex() != 0)
			{
				throw new IOException("Data page already referenced in a link: page index: " + parent.getPageIndex()+", next points to page index: "+parent.getNextDataPageIndex());
			}
			aLinkRefNext.set((int)parent.getNextDataPageIndex(), true);

			boolean firstElement = true;

			for (DataElement element : parent.getElements())
			{
				if (firstElement && BTree.compare(element.getKey(), aPrevKey) < 0 || !firstElement && BTree.compare(element.getKey(), aPrevKey) <= 0)
				{
					throw new IOException("Data elements has an incorrect order: key: " + toString(element.getKey())+", previous key: "+toString(aPrevKey));
				}
				aPrevKey = element.getKey();
				firstElement = false;
			}

			return aPrevKey;
		}
		else
		{
			IndexPage parent = (IndexPage)aParent;

			aPrevKey = integrityCheckOrder(mTree.loadPage(parent, parent.getFirstPageIndex()), aPrevKey, aLinkRefPrev, aLinkRefNext);

			for (IndexElement element : parent.getElements())
			{
				if (BTree.compare(element.mKey, aPrevKey) <= 0)
				{
					throw new IOException("Index elements has an incorrect order: key: " + toString(element.mKey)+", previous key: "+toString(aPrevKey));
				}

				aPrevKey = integrityCheckOrder(mTree.loadPage(parent, element.mPageIndex), element.mKey, aLinkRefPrev, aLinkRefNext);
			}

			return aPrevKey;
		}
	}


	private boolean integrityCheck(IndexPage aParent, long aIndex, BitSet aUsage, BitSet aRefs, HashSet<byte[]> aKeys) throws IOException, TreeIntegrityException
	{
		Page page;

		try
		{
			page = mTree.loadPage(aParent, aIndex);
		}
		catch (IOException e)
		{
			if (!e.getMessage().startsWith("Attemp to load a terminated page"))
			{
				mIntegrityCheckMessage = e.getMessage();
				return false;
			}
			throw e;
		}

		if (aUsage.get((int)page.getPageIndex()))
		{
			mIntegrityCheckMessage = "Page already used: page index: " + page.getPageIndex();
			return false;
		}
		aUsage.set((int)page.getPageIndex(), true);

		if (page instanceof IndexPage)
		{
			IndexPage indexPage = (IndexPage)page;

			if (aRefs.get((int)indexPage.getFirstPageIndex()))
			{
				mIntegrityCheckMessage = "Page already referenced: page index: " + indexPage.getFirstPageIndex();
				return false;
			}

			aRefs.set((int)indexPage.getFirstPageIndex(), true);

			if (!integrityCheck(indexPage, indexPage.getFirstPageIndex(), aUsage, aRefs, aKeys))
			{
				return false;
			}

			if (indexPage.mPageIndex != 0 && indexPage.getElements().isEmpty())
			{
				mIntegrityCheckMessage = "Page has only one child: page index: " + indexPage.mPageIndex;
				return false;
			}

			for (IndexElement element : indexPage.getElements())
			{
				if (aRefs.get((int)element.mPageIndex))
				{
					mIntegrityCheckMessage = "Page already referenced: page index: " + element.mPageIndex;
					return false;
				}

				if (aKeys.contains(element.mKey))
				{
					mIntegrityCheckMessage = "Index key already used: index key: " + element;
					return false;
				}
				aKeys.add(element.mKey);

				aRefs.set((int)element.mPageIndex, true);

				if (!integrityCheck(indexPage, element.mPageIndex, aUsage, aRefs, aKeys))
				{
					return false;
				}
			}
		}

		return true;
	}


	private String toString(byte [] v)
	{
		return Hex.encode(v);
	}


	void setIntegrityCheckMessage(String aMessage)
	{
		mIntegrityCheckMessage = aMessage;
	}

/*
	private boolean checkIndexNodeTypes() throws IOException
	{
		ByteBuffer in = ByteBuffer.createBigEndian(new byte[mTree.getPageSize()]);

		HashMap<Integer,Integer> pageTypes = new HashMap<Integer,Integer>();

		{
			RandomAccessFile file = new RandomAccessFile("d:/test.dat", "r");

			for (int pageIndex = 0; file.getFilePointer() < file.length(); pageIndex++)
			{
				file.readFully(in.array());
				switch ((char)in.getUnsignedByte(0))
				{
					case 'D': pageTypes.put(pageIndex, 0); break;
					case 'I': pageTypes.put(pageIndex, 1); break;
					case 'X': pageTypes.put(pageIndex, 2); break;
					default:  pageTypes.put(pageIndex, 3); break;
				}
			}

			file.close();
		}

		RandomAccessFile file = new RandomAccessFile("d:/test.dat", "r");

		try
		{
			for (int pageIndex = 0; file.getFilePointer() < file.length(); pageIndex++)
			{
				file.readFully(in.array());
				in.position(0);

				if ((char)in.getUnsignedByte() == 'I')
				{
					int count = in.getUnsignedShort();
					long first = in.getNumber(5);
					int type = pageTypes.get((int)first);
					for (int i = 0; i < count; i++)
					{
						in.skip(in.getUnsignedByte()+1);
						long pi = in.getNumber(5);
						if (type != pageTypes.get((int)pi))
						{
							in.position(1+2+5);
							String s = first + "/";
							switch (pageTypes.get((int)first))
							{
								case 0: s += "D"; break;
								case 1: s += "I"; break;
								case 2: s += "X"; break;
								case 3: s += "-"; break;
							}
							for (int j = 0; j < count; j++)
							{
								in.skip(in.getUnsignedByte()+1);
								pi = in.getNumber(5);
								s += " " + pi + "/";
								switch (pageTypes.get((int)pi))
								{
									case 0: s += "D"; break;
									case 1: s += "I"; break;
									case 2: s += "X"; break;
									case 3: s += "-"; break;
								}
							}

							mIntegrityCheckMessage = "Index contains mixed child nodes: page-index: " + pageIndex + ", structure: " + s;
							return false;
						}
					}
				}
			}
		}
		finally
		{
			file.close();
		}

		return true;
	}
 */
}