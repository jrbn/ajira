package nl.vu.cs.ajira.utils;

import java.io.Serializable;

/**
 * A simple table mapping longs to Objects. Some code derived from HashMap.
 * 
 * @param <V>
 *            the object type.
 */
public class LongMap<V> implements Serializable {

	private static final long serialVersionUID = 3669658604509818231L;

	private Entry<V>[] table;
	private int size;

	@SuppressWarnings("unchecked")
	public LongMap() {
		table = new Entry[65536];
	}

	@SuppressWarnings("unchecked")
	public LongMap(int initialSize) {
		// Make sure size is a multiple of 2.
		int sz = 2;
		while (sz < initialSize) {
			sz += sz;
		}
		table = new Entry[sz];
	}

	static public int hash(long h) {
		int hsh = (int) h ^ (int) (h >> 32);
		// code stolen from HashMap ...
		hsh ^= (hsh >>> 20) ^ (hsh >>> 12);
		return hsh ^ (hsh >>> 7) ^ (hsh >>> 4);
	}

	public int size() {
		return size;
	}

	public V get(long key) {
		int hash = hash(key);
		for (Entry<V> e = table[hash & (table.length - 1)]; e != null; e = e.next) {
			if (e.hash == hash && e.key == key)
				return e.value;
		}
		return null;
	}
	
	public Entry<V>[] getTable() {
		return table;
	}

	public boolean containsKey(long key) {
		return get(key) != null;
	}

	public V put(long key, V value) {
		int hash = hash(key);
		int i = hash & (table.length - 1);
		for (Entry<V> e = table[i]; e != null; e = e.next) {
			if (e.hash == hash && e.key == key) {
				V oldValue = e.value;
				e.value = value;
				return oldValue;
			}
		}

		addEntry(hash, key, value, i);
		return null;
	}

	void resize(int newCapacity) {
		@SuppressWarnings("unchecked")
		Entry<V>[] newTable = new Entry[newCapacity];
		transfer(newTable);
		table = newTable;
	}

	void transfer(Entry<V>[] newTable) {
		Entry<V>[] src = table;
		int newCapacity = newTable.length;
		for (int j = 0; j < src.length; j++) {
			Entry<V> e = src[j];
			if (e != null) {
				src[j] = null;
				do {
					Entry<V> next = e.next;
					int i = e.hash & (newCapacity - 1);
					e.next = newTable[i];
					newTable[i] = e;
					e = next;
				} while (e != null);
			}
		}
	}

	public V remove(long key) {
		Entry<V> e = removeEntryForKey(key);
		return (e == null ? null : e.value);
	}

	final Entry<V> removeEntryForKey(long key) {
		int hash = hash(key);
		int i = hash & (table.length - 1);
		Entry<V> prev = table[i];
		Entry<V> e = prev;

		while (e != null) {
			Entry<V> next = e.next;
			if (e.hash == hash && e.key == key) {
				size--;
				if (prev == e)
					table[i] = next;
				else
					prev.next = next;
				return e;
			}
			prev = e;
			e = next;
		}

		return e;
	}

	/**
	 * Removes all of the mappings from this map. The map will be empty after
	 * this call returns.
	 */
	public void clear() {
		Entry<V>[] tab = table;
		for (int i = 0; i < tab.length; i++)
			tab[i] = null;
		size = 0;
	}

	public boolean containsValue(Object value) {
		if (value == null)
			return false;

		Entry<V>[] tab = table;
		for (int i = 0; i < tab.length; i++)
			for (Entry<V> e = tab[i]; e != null; e = e.next)
				if (value.equals(e.value))
					return true;
		return false;
	}

	static public class Entry<V> implements Serializable {

		private static final long serialVersionUID = 6231648485442540166L;

		final long key;
		V value;
		Entry<V> next;
		final int hash;

		Entry(int h, long k, V v, Entry<V> n) {
			value = v;
			next = n;
			key = k;
			hash = h;
		}

		public final long getKey() {
			return key;
		}

		public final V getValue() {
			return value;
		}
		
		public Entry<V> getNext() {
			return next;
		}

		public final V setValue(V newValue) {
			V oldValue = value;
			value = newValue;
			return oldValue;
		}
	}

	void addEntry(int hash, long key, V value, int bucketIndex) {
		Entry<V> e = table[bucketIndex];
		table[bucketIndex] = new Entry<V>(hash, key, value, e);
		if (size++ >= 2 * table.length / 3) {
			resize(2 * table.length);
		}
	}
}
