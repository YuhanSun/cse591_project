package functions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class Arbitary_usage {

	public static void main(String[] args) throws IOException {
		
//		String str = "42CCC198	26400783	HiPEAC 2015	HiPEAC 2015 - 10th International Conference on High Performance and Embedded Architectures and Compilers (HiPEAC'2015)	Amsterdam, Netherlands - Kingdom of the Netherlands	http://www.hipeac.net/2015/amsterdam	2015/01/19	2015/01/21		2014/06/01		";
//		String[] l = str.split("\t");
//		System.out.println(l.length);
//		for(int i = 0;i<l.length;i++)	
//			System.out.println(l[i]);
//		System.out.println(l[11]);
		
		HashMap<String, double[]> hm = new HashMap<String, double[]>();
		double[] da = new double[2];
		da[0] = 3;
		da[1] = 4;
		hm.put("a", da);
		if(hm.get("b") == null)
			System.out.println(true);
		System.out.println(hm.get("b"));
		
//		String property = "OjAAAAEAAAAAAAMGEAAAAAAAAQACAAMABAAFAAYABwAIAAkACgALAAwADQAOAA8AEAARABIAEwAUABUAFgAXABgAGQAaABsAHAAdAB4AHwAgACEAIgAjACQAJQAmACcAKAApACoAKwAsAC0ALgAvADAAMQAyADMANAA1ADYANwA4ADkAOgA7ADwAPQA+AD8AQABBAEIAQwBEAEUARgBHAEgASQBKAEsATABNAE4ATwBQAFEAUgBTAFQAVQBWAFcAWABZAFoAWwBcAF0AXgBfAGAAYQBiAGMAZABlAGYAZwBoAGkAagBrAGwAbQBuAG8AcABxAHIAcwB0AHUAdgB3AHgAeQB6AHsAfAB9AH4AfwCAAIEAggCDAIQAhQCGAIcAiACJAIoAiwCMAI0AjgCPAJAAkQCSAJMAlACVAJYAlwCZAJoAmwCcAJ0AngCfAKAAoQCiAKMApAClAKYApwCoAKkAqwCtAK4AsACzALUAuAC5ALoAvAC9AL4AvwDAAMEAwgDFAMYAxwDIAMkAygDMANEA0wDUANUA1wDZANsA3ADeAOYA6ADpAOoA7ADtAPUA9gD5APoA/AAAAQEBAgEDAQQBBQEGAQcBCAEJAQoBCwEMAQ0BDwEQAREBEgETARQBFQEXARgBGQEaARsBHAEdAR4BHwEgASMBJAElASYBKAErASwBLQExATUBNwE7ATwBQwFEAUgBSwFMAU4BUAFRAVMBVAFVAVoBXwFgAWgBaQFvAXABfAGAAYEBggGDAYQBhQGGAYcBiAGJAYoBiwGMAY0BjgGPAZABkQGSAZUBlgGXAZkBmgGbAZ0BngGfAaIBowGkAaYBqgGuAa8BswG1AbcBvwHBAcMBxAHFAccByAHKAcsBzAHPAdAB1QHbAd4B4AHjAeoB6wHyAfYB9wH4AfsBAAIBAgICAwIEAgUCBgIHAgoCCwIMAg4CDwIQAhECEgIUAhUCFgIZAhoCGwIfAiICJAIlAicCKAIpAjACMQI0AjsCPAJEAkYCSAJKAk0CUAJRAlcCZQJpAnoCewKAAoECggKDAoQChQKGAocCiQKKAowCjgKQApYCmAKZApoCmwKeAqECpAKmAqcCqAKyArMCwALIAsoCywLSAvAC9wL4Av8CAAMBAwIDAwMEAwUDBgMHAwgDCQMKAwwDDQMOAw8DEwMVAxYDGQMaAyADIwMlAycDKAMrAy8DMAMxAzYDRwNMA00DUANSA1gDXANdA2IDYwNmA2oDawNuA28DegN+A4ADgQOCA4MDhAOFA4cDiAOJA4wDkgOTA5UDlwOZA5sDnwOlA6YDqAOtA64DsQO3A8IDxAPGA9AD3gPgA+UD6QPtA/oD/AMABAEEAgQDBAQEBQQGBAcECAQKBAwEDwQQBBMEFAQWBBkEGwQeBCEEIwQqBDAEMgQ2BDcEPQRHBEgESQRcBGMEcAR0BH0EgASBBIIEgwSEBIUEhgSHBIgEiQSKBIsEjASNBJAEkQSSBJUEmQSaBJwEngSgBKEEogSjBKYEqgStBLAEtAS1BLsEyATrBO4E8AT2BAAFAQUCBQMFBAUFBQYFCAUJBQoFDwUQBRMFFwUdBSQFJgUqBTMFNQU6BTwFQQVCBUkFTAVQBWAFbQVvBXkFfAWABYEFggWDBYUFhgWJBYoFiwWMBY0FkwWVBZwFnQWfBaEFpwWuBbAFtAW/BcYFyAXLBdMF1AXWBeEF8QX9BQAGAQYCBgMGBAYFBgYGBwYIBgkGCgYMBg0GDwYgBiwGPQZiBm0GfQaABoEGggaDBoYGiAaSBpMGlwaaBp8GyAbMBs4G1gYABwEHAgcDBwQHBgcIBwkHDQcOBw8HFgcbBy8HMgc3B4AHgQeCB4MHhAeHB4wHjgePB5MHmAegB6IHswe2B74H9Af8BwAIAQgCCAMIBQgHCAkIDQgUCBgIGQg1CDwITwhVCFoIaAh6CIAIgQiCCIMIhAiGCIsIjgiPCJEIkgiUCJUImgiiCKQIqgisCM4I1QgACQEJAgkECQUJBgkICQwJDgkUCRUJGAkpCS8JMwk5CYAJgQmCCYMJhAmFCYcJiAmOCY8JlQmWCbMJuQnECc0J/QkACgEKAwoGCgoKCwoMCg4KEQoTChQKJAolCicKKAouCjQKNgpOCl0KYApuCoAKgQqCCoMKhAqFCoYKiQqOCpsKowqoCtMK6goACwELAgsDCwQLBgsdCyYLLQs2C0MLTgtRC4ALgQuCC4MLhAuFC4kLiguOC5ELmQuwC8IL3QsADAEMAgwDDAQMCQwRDBUMGAwnDDIMTgyADIEMggyEDIcMiAyJDIsMkgyTDKoMuQzoDAANAQ0CDQMNBg0HDRENFA01DTsNSg1TDVYNYA1kDYANgQ2DDYQNhQ2KDY8NlA2fDaENqw2uDbQNtg23Db8N6Q0ADgEOAg4FDgcOCA4JDhkOGg4dDiQOMg43DjsOPg5KDk4OgA6CDoQOiA6JDosOkA6WDqkOrg7rDvkOAA8BDwIPBQ8MDxsPKQ8tD0cPSg9wD4APgQ+ED4UPiA+KD5APlw+nD7QPuQ8AEAEQAhAEEAUQFBAiECQQVhBXEIAQgRCDEIQQhRDGENQQABECEQcRDREkET4RZhGAEYERhRGKEYwRjhGbEZwRuhEAEgESAxIEEgUSBxIJEgoSDBIQEhESHBKAEoISgxKFEoYShxKIEpASnRKoErYSABMBEx0TPBNEE0YTSRNRE1sTgBOBE4ITgxOEE4YTjBOQE5ETmBObE50TnhPME84T1hP2EwAUARQCFAYUDRQRFCEUKhQzFDQUUBRcFGgUgBSBFIIUgxSJFI4UlRSwFMoUABUBFQIVAxUEFQUVCxUSFR8VIxUmFSgVgBWCFYMVhhWIFYkVjRWSFZMVlBWVFa8VwBXnFe4V9BUAFgEWAxYFFgcWHxYtFnAWgBaBFoIWiBb3FgAXARcCFwMXCBciFy0XOhdJF1AXbBeAF4EXjBeTF6AXsxcAGAIYBBgIGCAYMhiAGIEYghiGGIkYpRi2GLwYxRjeGAAZARkCGQQZBhkSGRcZLRkzGYAZghmGGYkZjxmQGZgZoRnIGckZABoHGgoaDBoOGhIaKRpCGoAagRqCGosajBqWGrUaABsBGwQbHRuAG4EbghuSG5MbsRsAHAQcDxwTHCAcRxxpHIAcgRyCHIQchxzEHAAdAR0EHQsdLh0/HYAdgR2FHcQdAB4BHgIeCB4LHhQeHx4pHioegB6CHoMeiB6LHpUeph4AHwUfCx8eHykfMh9JH1gfWR+AH4EfjR+4H9IfACAEIFgggCCDIIggiiCLIJAgmCCeILog/SAAIQohDCEUIRshISE+IUkhgCGBIYIhiCHnIQAiAiIDIoAigSKIIo8ipiK4IgAjAiMGIxQjfSOAI4EjiyOQI5MjnyO+I8Yj4CMAJAIkKCRgJIAkgSSCJIMkiiSNJAAlSCVqJYAlgiWFJYYlhyXRJQAmASYJJhUmIiYoJjwmdSaAJoEmnia6JscmACcBJwQnSSdlJ3ongCeBJ4InwCfZJwAoASgaKIAohyilKAApASkCKR4pNyl1KYApgimIKbYpvykAKgIqAyoHKgkqDCoWKnEqgCqHKooqkCqhKu0qACsBKw0rJiswK1UreCuAK4IrgyuHK6crwivLK9IrACwBLAMsBSwKLBosUiyALIEshCyeLJ8sAC0FLQ4tGi2ALYEthC2YLaktAC4BLgQuBS4/LoAugS7HLgAvBi8HL4Avgi8AMAEwAzAJMIAwgTCCMIQwizCMMNEwADEYMS4xgDGBMYIxjTEAMgQygDKEMoUyhzLoMgAzATMMMyAzKzOAM4EzgjOGM80zADSANIE0gzQANQE1IzVtNYA1gTWCNYM1hzWONQA2gDaBNoU2ADcHNwg3IjdNN083VzdjN4A3gjeIN6w3szcAOAE4BDiAOIM4hDiFOJw4qzgAOQE5DDk1OYA5gzmOOZk5mjkAOgE6BDpMOmY6gDrxOgA7JztOO4A7gjuJO407mjv8OwA8ATwCPAQ8DDwzPFc8gDyBPII8yzztPAA9BD0JPQ89gD2CPY09nT2fPdM9AD4EPgU+QT6APoE+gj6DPoQ+iT6OPr4+9j4APwE/Az8EPwg/Lz85P0c/gD+DP6I/";
//		RoaringBitmap rb = new RoaringBitmap();
//	    byte[] nodeIds = property.getBytes();
//	    ByteArrayInputStream bais = new ByteArrayInputStream(nodeIds);
//	    rb.deserialize(new DataInputStream(bais));
//	    System.out.println(rb.toString());
		
//		RoaringBitmap rb = new RoaringBitmap();
//		rb.add(0);
//		rb.add(1);
//		String str = OwnMethods.Serialize_RoarBitmap_ToString(rb);
//		ImmutableRoaringBitmap ir = OwnMethods.Deserialize_String_ToRoarBitmap(str);
//		System.out.println(rb.equals(ir));
//		
//		RoaringBitmap r2 = new RoaringBitmap();
//		r2.add(0);
//		r2.add(1);
//		
//		System.out.println(rb.equals(r2));
//		String filepath = "/home/yuhansun/Documents/share/Real_data/Patents/topology_sort.txt";
//		ArrayList<Integer> al = OwnMethods.ReadTopoSequence(filepath);
//		System.out.println(al.get(0));
//		String query = null;
//		String result = null;
//		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
//		query = String.format("match (a)-->(b) where id(b) = %d return id(a)",4040605);
//		result = p_neo.Execute(query);
//		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//		
//		for(int j_index = 0;j_index<jsonArr.size();j_index++)
//		{
//			long id = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray().get(0).getAsLong();
//			OwnMethods.Println(id);
//		}
//		MyRectangle rec = null;
//		if(rec == null)
//			OwnMethods.Println(true);
		
//		Config p_con = new Config();
//		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
//		GeoReach p_geo = new GeoReach();
//		long graph_size = OwnMethods.GetNodeCount("Patents");
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, 500);
//		Iterator<String> iter = hs.iterator();
//		while(iter.hasNext())
//		{
//			long id = 4*graph_size+Long.parseLong(iter.next());
//			System.out.println(id);
//			String query = String.format("match (n) where id(n) =%d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", id, p_con.GetLongitudePropertyName(), p_con.GetLatitudePropertyName(), p_con.GetRMBR_minx_name(), p_con.GetRMBR_miny_name(), p_con.GetRMBR_maxx_name(), p_con.GetRMBR_maxy_name());
//			String result = p_neo.Execute(query);
//			JsonArray jarr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//			jarr = jarr.get(0).getAsJsonObject().get("row").getAsJsonArray();
//			p_geo.UpdateTraverse(id, jarr);
//		}
		
		
//		Config p_con = new Config();
//		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
//		String query = null;
//		String result = null;
//		query = String.format("match (n)-->(b) where id(b) = %d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", 5000000, p_con.GetLongitudePropertyName(), p_con.GetLatitudePropertyName(), p_con.GetRMBR_minx_name(), p_con.GetRMBR_miny_name(), p_con.GetRMBR_maxx_name(), p_con.GetRMBR_maxy_name());
//		result = p_neo.Execute(query);
//		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//		
//		for(int j_index = 0;j_index<jsonArr.size();j_index++)
//		{
//			JsonArray jArr_start = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray();
//			System.out.println(jArr_start.toString());
//		}
		
//		GeoReach p_geo = new GeoReach();
//		p_geo.UpdateDeleteEdge(4000000, 5000000);
//		double x = 1.000/3;
//		double y = 1.00/3;
//		System.out.println(x-y);
//		if(x-y == 0)
//			OwnMethods.Println(true);
//		else
//			OwnMethods.Println(false);
//		System.out.println(x);
//		String str = String.format("%.8f", x);
//		System.out.println(str);
		
//		String str = "OjAAAAEAAAAAAI0AEAAAABQAIQAkAC4ANAA8AD8AQABMAE8AWgBhAGUAZwBoAG8AcgB3AHoA/AA8AUQBtQHgAesB+wF7ArMCEwM2A8oDdASgBKQEvgXIBi0HjwiICQwKiQoYDIAMggySDOIMEQ01DWANlA2cDasNGw4FDwwPig+QD4MQlxCFEZsRARIcEoIShxKIEpASABMzFJQVkxcAGDMZABphGpYagBuAHAsdxB2AHgAfCx9ZHwAggyC6IAIjkCOAJAAmACiAKb8pACoALBcsgC3HLgYvgC8DMA42ADgCOIA6iTsAPAA9gD4APwtAEUATQBZAG0AjQCVAL0AxQDVASkJAQ8BDAERARkBHQEjASEBMgEzATcBOBFAKUAxQDlAVUANUIFRQVABV";
////		RoaringBitmap r = new RoaringBitmap();
////		r.add(2);
////		r.add(3);
//		ImmutableRoaringBitmap r = OwnMethods.Deserialize_String_ToRoarBitmap(str);
//		System.out.println(r);
//		System.out.println(r.getCardinality());
		
		//OwnMethods.RestartNeo4jClearCache("Patents");
		
//		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
//		for(int name_index = 0;name_index<datasource_a.size();name_index++)
//		{
//			String datasource = datasource_a.get(name_index);
//			SpatialIndex.DropTable(datasource, "_zipf");
//		}
		
//		String datasource = "Patents";
//		BatchInserter inserter = null;
//		BufferedReader reader = null;
//		File file = null;
//		Map<String, String> config = new HashMap<String, String>();
//		config.put("dbms.pagecache.memory", "5g");
//		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
//		int node_count = OwnMethods.GetNodeCount(datasource);
//		int ratio = 60;
//		//{
//			long offset = ratio / 20 * node_count;
//			try
//			{
//				
//			}
//		String datasource = "Patents";
//		int graph_size = OwnMethods.GetNodeCount(datasource);
//		double experiment_node_count = 500;
//		String graph_label = "Graph_Random_80";
//		double spatial_total_range = 1000;
//		System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//		OwnMethods.GetNodeCount(datasource);
//		
//		Random r = new Random();		
//		double j = 1;
//		while(true)
//		{
//			System.out.println(OwnMethods.ClearCache());
//			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//			Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + 80);
//			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
//			ArrayList<Double> lx = new ArrayList<Double>();
//			ArrayList<Double> ly = new ArrayList<Double>();
//			
//			int true_count = 0;
//			
//			for(int i = 0;i<al.size();i++)
//			{
//				lx.add(r.nextDouble()*(1000-rect_size));
//				ly.add(r.nextDouble()*(1000-rect_size));
//			}
//			
//			long time = 0;
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				
//				double x = lx.get(i);
//				double y = ly.get(i);
//								
//				MyRectangle rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
//				try 
//				{
//					long start = System.currentTimeMillis();
//					boolean result = spareach.ReachabilityQuery(id, rect);
//					time += (System.currentTimeMillis() - start);
//					System.out.println(i);
//					System.out.println(result);
//					
//					if(result)
//						true_count+=1;
//				} 
//				catch(Exception e)
//				{
//					e.printStackTrace();
//					i = i-1;
//				}		
//				
//			}
//			
//			spareach.Disconnect();
//			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//			
//			double a_time = time/experiment_node_count;
//			
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/test.txt", true, j+"\t"+a_time+"\t"+true_count+"\n");
//			if(j<1)
//				j*=10;
//			else
//				j+=10;
//			
//			if(j>60)
//				break;
//			
//			if(a_time>3000)
//				break;
//		}
		
		//for(int i = 0;i<1;i++)
			//System.out.println(i);
		
//		double x = 53.283532305734234;
//		double y = 65.15745003013664;
//		
//		MyRectangle spatialrange = new MyRectangle(0,0,1000,1000);
//		double size = 331.66247903553995;
//		GeoReach_Integrate geo = new GeoReach_Integrate(spatialrange, 5);
//		MyRectangle rect = new MyRectangle(x,y,x+size, y+size);
//		System.out.println(geo.ReachabilityQuery(5219357, rect));
		
	}

}
