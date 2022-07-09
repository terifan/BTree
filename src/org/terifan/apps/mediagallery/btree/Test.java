package org.terifan.apps.mediagallery.btree;

import java.io.File;
import java.util.Base64;
import java.util.Random;


public class Test
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random();

			File file = new File("d:\\btree.db");
			file.delete();

			{
				BTree tree = new BTree(new FilePageStore(file, false, 4096));
				for (int i = 0; i < 100; i++)
				{
					byte[] key = new byte[rnd.nextInt(20)];
					byte[] value = new byte[rnd.nextInt(20)];
					rnd.nextBytes(key);
					rnd.nextBytes(value);
//					tree.put(key, value);
					tree.put(Base64.getEncoder().encode(key), Base64.getEncoder().encode(value));
				}
				tree.close();
			}

			{
				BTree tree = new BTree(new FilePageStore(file, false, 4096));
				tree.iterator().forEach(k -> System.out.println(new String(k.mKey) + "=" + new String(k.mContent)));
				tree.close();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
