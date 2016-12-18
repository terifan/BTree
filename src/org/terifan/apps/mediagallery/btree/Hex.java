package org.terifan.apps.mediagallery.btree;


class Hex
{
	private final static String [] DIGITS = {"0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"};


	public static String encode(byte [] aBuffer)
	{
		StringBuilder s = new StringBuilder(2*aBuffer.length);
		for (int i = 0; i < aBuffer.length; i++)
		{
			int b = aBuffer[i] & 255;
			s.append(DIGITS[b>>4]).append(DIGITS[b&15]);
		}
		return s.toString();
	}
}