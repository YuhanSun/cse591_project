package functions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.sun.jersey.api.client.WebResource;

public class Batch_Inserter {
	
	private String longitude_property_name;
	private String latitude_property_name;
	private String RMBR_minx_name;
	private String RMBR_miny_name;
	private String RMBR_maxx_name;
	private String RMBR_maxy_name;
	public String suffix;
	
	public Batch_Inserter()
	{
		Config config = new Config();
		suffix = config.GetSuffix();
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		RMBR_minx_name = config.GetRMBR_minx_name();
		RMBR_miny_name = config.GetRMBR_miny_name();
		RMBR_maxx_name = config.GetRMBR_maxx_name();
		RMBR_maxy_name = config.GetRMBR_maxy_name();
	}
	
	
	
	
	
	public static void CreateUniqueConstraint()
	{
		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		WebResource resource = p_neo.GetCypherResource();
		p_neo.Execute(resource, "create constraint on (n:Reachability_Index) assert n.id is unique");
		p_neo.Execute(resource, "create constraint on (n:Reachability_Index) assert n.scc_id is unique");

		for(int i = 0;i<100;i+=20)
		{
			p_neo.Execute(resource, "create constraint on (n:Graph_Random_" + Integer.toString(i) + ") assert n.id is unique");
			//p_neo.Execute(resource, "create constraint on (n:RTree_Random_" + Integer.toString(i) + ") assert n.id is unique");
		}
	}
	
	public static void UpdateError()
	{		
		for(int ratio = 20;ratio<100;ratio+=20)
		{
			BatchInserter inserter = null;
			BufferedReader reader = null;
			Map<String, String> config = new HashMap<String, String>();
			config.put("dbms.pagecache.memory", "5g");
			long offset = 3774768 * ratio/20;
			try
			{
				inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.3.3/data/graph.db").getAbsolutePath(),config);
				
				File file = null;
				String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/"+ratio;
				
				file = new File(filepath + "/entity.txt");	
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				
				tempString = reader.readLine();
				
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					double minx = Double.parseDouble(l[6]);
					if(minx < 0)
						continue;
					double miny = Double.parseDouble(l[7]);
					double maxx = Double.parseDouble(l[8]);
					double maxy = Double.parseDouble(l[9]);
					int id = Integer.parseInt(l[0]);
					inserter.setNodeProperty(id + offset, "RMBR_minx", minx);
					inserter.setNodeProperty(id + offset, "RMBR_miny", miny);
					inserter.setNodeProperty(id + offset, "RMBR_maxx", maxx);
					inserter.setNodeProperty(id + offset, "RMBR_maxy", maxy);
				}
				reader.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(inserter!=null)
					inserter.shutdown();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e)
					{					
					}
				}
			}
		}
	}
	
	public static void LoadReachabilityIndex(String datasource)
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		BufferedReader reader_reachFrom = null;
		BufferedReader reader_reachTo = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.3.3/data/graph.db";
		
		try
		{
			Map<Integer,Integer> table = new HashMap<Integer, Integer>();
			file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/table.txt");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine())!=null)
			{		
				String[] l = tempString.split("\t");
				int first = Integer.parseInt(l[0]);
				int second = Integer.parseInt(l[1]);
				table.put(second, first);
			}
			reader.close();
			
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			File file_reachFrom = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/reachFromIndex.txt");
			File file_reachTo = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/reachToIndex.txt");
			reader_reachFrom = new BufferedReader(new FileReader(file_reachFrom));
			reader_reachTo = new BufferedReader(new FileReader(file_reachTo));
			String str_reachFrom = null, str_reachTo = null;
			Label Reach_Index_Label = DynamicLabel.label("Reachability_Index");
			while(((str_reachFrom = reader_reachFrom.readLine())!=null)&&((str_reachTo = reader_reachTo.readLine())!=null))
			{
				String[] l_rF = str_reachFrom.split("\t");
				String[] l_rT = str_reachTo.split("\t");
				int scc_id = Integer.parseInt(l_rF[0]);
				int id = table.get(scc_id);
				Map<String, Object> properties = new HashMap<String, Object>();
				properties.put("scc_id", scc_id);
				properties.put("id", id);
				
				if(l_rF.length>1)
				{
					int[] l = new int[l_rF.length-1];
					for(int i = 0;i<l.length;i++)
						l[i] = Integer.parseInt(l_rF[i+1]);
					properties.put("reachFrom", l);
				}
				if(l_rT.length>1)
				{
					int[] l = new int[l_rT.length-1];
					for(int i = 0;i<l.length;i++)
						l[i] = Integer.parseInt(l_rT[i+1]);
					properties.put("reachTo", l);
				}
				inserter.createNode(id, properties, Reach_Index_Label);
			}
		}
		catch(IOException e)
		{
			if(inserter!=null)
				inserter.shutdown();
			e.printStackTrace();
		}
		finally
		{
			if(inserter!=null)
				inserter.shutdown();
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
			if(reader_reachFrom!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
			if(reader_reachTo!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
	}
	
	public void LoadGraph(String datasource, int ratio, String filesuffix)
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "6g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.3.3/data/graph.db";
		//for(int ratio = 20;ratio<100;ratio+=20)
		int node_count = OwnMethods.GetNodeCount(datasource);
		{
			int offset = ratio / 20 * node_count;
			RelationshipType graph_rel = DynamicRelationshipType.withName("LINK");
			try
			{
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(), config);
				
				Label graph_label = DynamicLabel.label("Graph_" + ratio+suffix);
				
				String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/"+filesuffix+"/" + ratio;
				reader = null;
				file = null;
				
				file = new File(filepath + "/entity.txt");
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				while((tempString = reader.readLine())!=null)
				{
					Map<String, Object> properties = new HashMap<String, Object>();
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					properties.put("id", id);
					
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial==1)
					{
						double lon = Double.parseDouble(l[2]);
						double lat = Double.parseDouble(l[3]);
						properties.put(longitude_property_name, lon);
						properties.put(latitude_property_name, lat); 
					}
					
					inserter.createNode(id + offset, properties, graph_label);							
				}
				reader.close();
				
			}
				
			catch(IOException e)
			{
				e.printStackTrace();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e1)
					{					
					}
				}
				if(inserter!=null)
					inserter.shutdown();
			}
			finally
			{
				if(inserter!=null)
					inserter.shutdown();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e)
					{					
					}
				}
			}
			
			try
			{
				//graph relationships
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(), config);
				file = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/graph.txt");											
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				reader.readLine();
				while((tempString = reader.readLine())!=null)
				{	
					String[] l = tempString.split(" ");
					long start = Long.parseLong(l[0]);
					long count = Long.parseLong(l[1]);
					if(count == 0)
						continue;
					for(int i = 2;i<l.length ;i++)
					{
						long end = Long.parseLong(l[i]);
						inserter.createRelationship(start + offset, end + offset, graph_rel, null);
					}
				}
				reader.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e1)
					{					
					}
				}
				if(inserter!=null)
					inserter.shutdown();
			}
			finally
			{
				if(inserter!=null)
					inserter.shutdown();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e)
					{					
					}
				}
			}
			
		}
	}
	
	public void LoadGraph(String datasource)
	{
		int node_count = OwnMethods.GetNodeCount(datasource);
		BatchInserter inserter = null;
		BufferedReader[] readers = new BufferedReader[12];	
		Label graph_label = DynamicLabel.label("Graph");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.3.3/data/graph.db";
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "6g");
		
		String[] filenames = new String[12];
		for( int i = 0;i<4;i++)
		{
			filenames[i] = "/home/yuhansun/Documents/Real_data/"+datasource+"/Random_spatial_distributed/" + (i+1)*20 + "/entity.txt";
		}
		for( int i = 4;i<8;i++)
		{
			filenames[i] = "/home/yuhansun/Documents/Real_data/"+datasource+"/Clustered_distributed/" + (i-3)*20 + "/entity.txt";
		}
		for( int i = 8;i<12;i++)
		{
			filenames[i] = "/home/yuhansun/Documents/Real_data/"+datasource+"/Zipf_distributed/" + (i-7)*20 + "/entity.txt";
		}
		
		File file = null;
		BufferedReader reader = null;
		RelationshipType graph_rel = DynamicRelationshipType.withName("LINK");
		
		
		try
		{
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(), config);
			for (int i = 0;i<12;i++)
			{
				readers[i] = new BufferedReader(new FileReader(new File(filenames[i])));
			}
			String[] temp = new String[12];
			for(int i = 0;i<12;i++)
			{
				temp[i] = readers[i].readLine();
//				System.out.println(temp[i]);
			}
			
			int id = 0;
			while((temp[0] = readers[0].readLine())!=null)
			{
				for(int i = 1;i<12;i++)
					temp[i] = readers[i].readLine();
				Map<String, Object> properties = new HashMap<String, Object>();
				properties.put("id", id);
				
				for(int i = 0;i<4;i++)
				{
					String[] l = temp[i].split(" ");
					
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial==1)
					{
						double lon = Double.parseDouble(l[2]);
						double lat = Double.parseDouble(l[3]);
						properties.put("longitude_"+(i+1)*20+"_random", lon);
						properties.put("latitude_"+(i+1)*20+"_random", lat); 
					}
				}
				for(int i = 4;i<8;i++)
				{
					String[] l = temp[i].split(" ");
					
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial==1)
					{
						double lon = Double.parseDouble(l[2]);
						double lat = Double.parseDouble(l[3]);
						properties.put("longitude_"+(i-3)*20+"_clustered", lon);
						properties.put("latitude_"+(i-3)*20+"_clustered", lat); 
					}
				}
				for(int i = 8;i<12;i++)
				{
					String[] l = temp[i].split(" ");
					
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial==1)
					{
						double lon = Double.parseDouble(l[2]);
						double lat = Double.parseDouble(l[3]);
						properties.put("longitude_"+(i-7)*20+"_zipf", lon);
						properties.put("latitude_"+(i-7)*20+"_zipf", lat); 
					}
				}
				inserter.createNode(id + node_count, properties, graph_label);
				id++;
			}
			
			for(int i = 0;i<12;i++)
				readers[i].close();
			
			
			//graph relationships
			file = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/graph.txt");										
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			reader.readLine();
			while((tempString = reader.readLine())!=null)
			{	
				String[] l = tempString.split(" ");
				long start = Long.parseLong(l[0]);
				long count = Long.parseLong(l[1]);
				if(count == 0)
					continue;
				for(int i = 2;i<l.length ;i++)
				{
					long end = Long.parseLong(l[i]);
					inserter.createRelationship(start + node_count, end + node_count, graph_rel, null);
				}
			}
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
			if(inserter!=null)
				inserter.shutdown();
			for(int i = 0;i<12;i++)
			{
				if(readers[i]!=null)
					try {
						readers[i].close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}
	}
	
	public static void SetNull(String datasource, int ratio, String propertyname)
	{
		long node_count;
		BatchInserter inserter = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.3.3/data/graph.db";
				
		{
			try
			{
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
				node_count = OwnMethods.GetNodeCount(datasource);
				long offset = ratio / 20 * node_count;
				for(int id = 0;id<node_count;id++)
				{
					if(inserter.getNodeProperties(id+offset).containsKey(propertyname))
						inserter.removeNodeProperty(id + offset, propertyname);
				}
			}
			catch(Exception e)
			{
				if(inserter!=null)
					inserter.shutdown();
				e.printStackTrace();
			}
			finally
			{
				if(inserter!=null)
					inserter.shutdown();
				OwnMethods.ClearCache();
			}
		}
	}

	public static void main(String[] args) 
	{	
		try
		{
			OwnMethods.PrintArray(args);
			Batch_Inserter p_batch = new Batch_Inserter();
			if(args[0].equals("LoadGraph"))
			{
				String datasource = args[1];
				p_batch.LoadGraph(datasource);
			}
			if(args[0].equals("LoadReachabilityIndex"))
			{
				String datasource = args[1];
				LoadReachabilityIndex(datasource);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
}
