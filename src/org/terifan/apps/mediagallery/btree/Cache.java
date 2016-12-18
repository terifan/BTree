package org.terifan.apps.mediagallery.btree;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;


/**
 * The Cache class is an implementation of a MRU cache with a specified
 * capacity. When the size of the Cache exceeds the capacity, items will be
 * removed to accommodate the new items. Adding one item may result in
 * multiple items being removed.<p>
 *
 * The capacity and size parameters may be actual memory sizes, estimated sizes
 * or counts. A simplified version of the put method exists when size equals
 * one, as in a count based Cache.<p>
 *
 * CacheStateListeners can be used to monitor how the cache receives removes
 * items.<p>
 *
 * @param <K>
 *   the key type used by this cache
 * @param <V>
 *   the value type used by this cache
 */
public class Cache<K,V> implements Iterable<K>
{
	private long mCapacity;
	private long mUsedSize;
//	private ArrayList<CacheStateListener<K,V>> mListeners;
	private LinkedList<K> mCacheOrder;
	private HashMap<K,Entry<K,V>> mKeyValueMap;
//	private CacheBackend<K,V> mCacheBackend;
	private int mExpireTime;


	public class Entry<K,V>
	{
		private K key;
		private V value;
		private long time;
		private long size;
	}


	/**
	 * Constructs a new standalone Cache object with a certain capacity.
	 *
	 * @param aCapacity
	 *   capacity of this Cache. Items will be removed when the total size
	 *   exceeds this value.
	 */
	public Cache(long aCapacity)
	{
		mCapacity = aCapacity;
		mKeyValueMap = new HashMap<>();
		mCacheOrder = new LinkedList<>();
//		mListeners = new ArrayList<>();
		mExpireTime = Integer.MAX_VALUE;
	}


	public int getExpireTime()
	{
		return mExpireTime;
	}


	public void setExpireTime(int aExpireTime)
	{
		mExpireTime = aExpireTime;
	}


//	/**
//	 * Gets the CacheBackend for this Cache.
//	 *
//	 * @return
//	 *   the CacheBackend or null if none exists
//	 */
//	public CacheBackend<K,V> getCacheBackend()
//	{
//		return mCacheBackend;
//	}
//
//
//	/**
//	 * Sets the CacheBackend for this Cache.
//	 *
//	 * @param aCacheBackend
//	 *   the CacheBackend or null
//	 */
//	public void setCacheBackend(CacheBackend<K,V> aCacheBackend)
//	{
//		mCacheBackend = aCacheBackend;
//	}


//	/**
//	 * Adds a CacheStateListener to this cache.
//	 *
//	 * @param aCacheStateListener
//	 *   the listener instance.
//	 */
//	public void addCacheStateListener(CacheStateListener<K,V> aCacheStateListener)
//	{
//		mListeners.add(aCacheStateListener);
//	}
//
//
//	/**
//	 * Removes a CacheStateListener from this cache.
//	 *
//	 * @param aCacheStateListener
//	 *   the listener instance.
//	 */
//	public void removeCacheStateListener(CacheStateListener<K,V> aCacheStateListener)
//	{
//		mListeners.remove(aCacheStateListener);
//	}


	/**
	 * Sets the capacity of this Cache.<p>
	 *
	 * Note: when reducing capacity items will be removed from this Cache
	 * instance without effecting other joined instances.
	 *
	 * @param aCapacity
	 *   capacity of this Cache. Items will be removed from the Cache when the
	 *   total size exceeds this value.
	 */
	public synchronized void setCapacity(long aCapacity)
	{
		mCapacity = aCapacity;

		shrink();
	}


	/**
	 * Gets the capacity of this Cache.
	 *
	 * @return
	 *   the capacity of this Cache
	 */
	public long getCapacity()
	{
		return mCapacity;
	}


	/**
	 * Associates the specified value with the specified key in this map.<p>
	 *
	 * This method calls all CacheStateListeners of this Cache if the Cache is
	 * full and aKey is not already in the Cache.<p>
	 *
	 * This method will always reject all items that exceed the capacity.
	 *
	 * @param aKey
	 *   Key with which the specified value is to be associated. Must not be
	 *   null or zero length.
	 * @param aValue
	 *   Value to be associated with the specified key.
	 * @param aItemSize
	 *   the size of the item.
	 * @return
	 *   Previous value associated with specified key, or null if there was no
	 *   mapping for key. A null return can also indicate that the map
	 *   previously associated null with the specified key.
	 */
	public synchronized V put(K aKey, V aValue, long aItemSize)
	{
		if (aItemSize > mCapacity)
		{
			return null;
		}

		V prevValue;

		if (mKeyValueMap.containsKey(aKey))
		{
			mCacheOrder.remove(aKey);
			mCacheOrder.addFirst(aKey);

			Entry<K,V> entry = mKeyValueMap.get(aKey);

			prevValue = entry.value;

			mUsedSize += aItemSize - entry.size;

			entry.value = aValue;
			entry.size = aItemSize;
			entry.time = System.currentTimeMillis();

			shrink();

//			for (CacheStateListener listener : mListeners)
//			{
//				listener.entryUpdated(this, aKey, aValue);
//			}
		}
		else
		{
			prevValue = null;

			Entry entry = new Entry();
			entry.key = aKey;
			entry.value = aValue;
			entry.size = aItemSize;
			entry.time = System.currentTimeMillis();

			mUsedSize += aItemSize;
			mCacheOrder.addFirst(aKey);
			mKeyValueMap.put(aKey, entry);

			shrink();

//			for (CacheStateListener listener : mListeners)
//			{
//				listener.entryAdded(this, aKey, aValue);
//			}
		}

		return prevValue;
	}


	private void shrink()
	{
		while (mUsedSize > mCapacity && mKeyValueMap.size() > 0)
		{
			removeImpl(mCacheOrder.getLast(), true);
		}

		if (mExpireTime < Integer.MAX_VALUE)
		{
			long threshold = System.currentTimeMillis() - mExpireTime;

			while (mKeyValueMap.size() > 0)
			{
				K last = mCacheOrder.getLast();
				Entry entry = mKeyValueMap.get(last);
				if (entry == null || entry.time > threshold)
				{
					break;
				}
				removeImpl(last, true);
			}
		}
	}


	/**
	 * Returns the value to which the specified key is mapped in this identity
	 * hash map, or null if the map contains no mapping for this key. A return
	 * value of null does not necessarily indicate that the map contains no
	 * mapping for the key; it is also possible that the map explicitly maps
	 * the key to null. The containsKey method may be used to distinguish
	 * these two cases.<p>
	 *
	 * Getting a value will cause a reorder of items. The retrieved key will be
	 * moved to the top of the cache.
	 *
	 * @param aKey
	 *   Key whose associated value is to be returned. Must not be null or
	 *   zero length.
	 * @return
	 *   The value to which this map maps the specified key, or null if the
	 *   map contains no mapping for this key.
	 */
	public synchronized V get(K aKey)
	{
		Entry<K, V> entry = mKeyValueMap.get(aKey);

		if (entry != null)
		{
			mCacheOrder.remove(aKey);
			mCacheOrder.addFirst(aKey);

			return entry.value;
		}
//		else if (mCacheBackend != null && mCacheBackend.retrieveItem(this, aKey))
//		{
//			return get(aKey);
//		}

		return null;
	}


	/**
	 * Returns the value to which the specified key is mapped in this identity
	 * hash map, or null if the map contains no mapping for this key. A return
	 * value of null does not necessarily indicate that the map contains no
	 * mapping for the key; it is also possible that the map explicitly maps
	 * the key to null. The containsKey method may be used to distinguish
	 * these two cases.<p>
	 *
	 * This method will not effect the order of items.
	 *
	 * @param aKey
	 *   Key whose associated value is to be returned. Must not be null or
	 *   zero length.
	 * @return
	 *   The value to which this map maps the specified key, or null if the
	 *   map contains no mapping for this key.
	 */
	public synchronized V peek(K aKey)
	{
		Entry<K,V> entry = mKeyValueMap.get(aKey);
		if (entry == null)
		{
			return null;
		}
		return entry.value;
	}


	/**
	 * Returns true if this map contains a mapping for the specified key.
	 *
	 * @param aKey
	 *	 Key whose presence in this map is to be tested.
	 * @return
	 *	 True if this map contains a mapping for the specified key.
	 */
	public synchronized boolean containsKey(K aKey)
	{
		boolean b = mKeyValueMap.containsKey(aKey);

		if (b)
		{
			mCacheOrder.remove(aKey);
			mCacheOrder.addFirst(aKey);
		}
//		else if (mCacheBackend != null)
//		{
//			// the CacheBackend is responsible for re-inserting the item which
//			// positions it at the top of the MRU so no reorder is necessary.
//
//			b = mCacheBackend.retrieveItem(this, aKey);
//		}

		return b;
	}


	/**
	 * Moves the key provided to the top of the MRU list but only if the key
	 * already exists in the list.
	 *
	 * @param aKey
	 *	 Key that should be bumped.
	 * @return
	 *	 True if this map contains a mapping for the specified key.
	 */
	public synchronized boolean bump(K aKey)
	{
		if (mCacheOrder.remove(aKey))
		{
			mCacheOrder.addFirst(aKey);
			return true;
		}
		return false;
	}


	/**
	 * Removes the mapping for this key from this map if it is present.
	 *
	 * @param aKey
	 *    Key whose mapping is to be removed from the map.
	 * @return
	 *    Previous value associated with specified key, or null if there
	 *    was no mapping for key.
	 */
	public synchronized V remove(K aKey)
	{
		return removeImpl(aKey, false);
	}


	public synchronized void removeAll(Collection<K> aKeys)
	{
		for (K key : aKeys)
		{
			removeImpl(key, false);
		}
	}


	private synchronized V removeImpl(K aKey, boolean aDropped)
	{
		Entry<K,V> entry = mKeyValueMap.remove(aKey);

		if (entry == null)
		{
			return null;
		}

		mCacheOrder.remove(aKey);
		mUsedSize -= entry.size;

		return entry.value;
	}


	/**
	 * Returns the number of key-value mappings in this map.
	 *
	 * @return
	 *   the number of key-value mappings in this map.
	 */
	public synchronized int size()
	{
		return mKeyValueMap.size();
	}


	/**
	 * Returns the total item size of this map.
	 *
	 * @return
	 *   the total item size of this map.
	 */
	public synchronized long getUsedSize()
	{
		return mUsedSize;
	}


	/**
	 * Removes all mappings from this map. This method iterates the contents of
	 * this cache removing items from the end. Listeners will be called for
	 * every item removed.
	 */
	public synchronized void clear()
	{
		while (mCacheOrder.size() > 0)
		{
			remove(mCacheOrder.getLast());
		}

		mUsedSize = 0;
		mKeyValueMap.clear();
		mCacheOrder.clear();
	}


	/**
	 * Removes all mappings from this map without calling any listeners or the
	 * CacheBackend if one exists.
	 */
	public synchronized void clearQuiet()
	{
		mUsedSize = 0;
		mKeyValueMap.clear();
		mCacheOrder.clear();
	}


	/**
	 * Returns true if this map contains no key-value mappings.
	 *
	 * @return
	 *   true if this map contains no key-value mappings.
	 */
	public synchronized boolean isEmpty()
	{
		return mKeyValueMap.isEmpty();
	}


//	/**
//	 * Returns a collection view of the values contained in this map.
//	 *
//	 * @return
//	 *   a collection view of the values contained in this map.
//	 */
//	public synchronized Collection<V> values()
//	{
//		return mKeyValueMap.values();
//	}


	/**
	 * Returns a set view of the keys contained in this map.
	 *
	 * @return
	 *   a set view of the keys contained in this map.
	 */
	public synchronized Set<K> keySet()
	{
		return mKeyValueMap.keySet();
	}


//	/**
//	 * Returns true if this map maps one or more keys to the specified value.
//	 *
//	 * @param aValue
//	 *   the value to look for
//	 * @return
//	 *   true if value exists
//	 */
//	public synchronized boolean containsValue(V aValue)
//	{
//		return mKeyValueMap.containsValue(aValue);
//	}
//
//
//	/**
//	 * Returns a collection view of the mappings contained in this map.
//	 *
//	 * @return
//	 *   a collection view of the mappings contained in this map.
//	 */
//	public synchronized Set<Map.Entry<K,V>> entrySet()
//	{
//		return mKeyValueMap.entrySet();
//	}


	/**
	 * Returns an iterator for the keys in this Cache. The iterator is sorted
	 * with the most recently used item first and the least recently used item
	 * last.<p>
	 *
	 * Note: the Iterator must not be used to remove items.
	 */
	@Override
	public synchronized Iterator<K> iterator()
	{
		return mCacheOrder.iterator();
	}
}