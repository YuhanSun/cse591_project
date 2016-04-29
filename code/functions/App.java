package functions;

import org.roaringbitmap.RoaringBitmap;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	RoaringBitmap r3 = RoaringBitmap.bitmapOf();
		for(int i = 0;i<5000;i++)
			r3.add(i*2);
		//System.out.println(r3);
		r3.runOptimize();
		
        String serializedstring = OwnMethods.Serialize_RoarBitmap_ToString(r3);
		
		System.out.println(serializedstring);
		
		System.out.print(OwnMethods.Deserialize_String_ToRoarBitmap(serializedstring));
    }
}
