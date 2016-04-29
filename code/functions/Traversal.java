package functions;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Traversal implements ReachabilityQuerySolver	{
	
	//used in query procedure in order to record visited vertices
	public Set<Integer> VisitedVertices = new HashSet<Integer>();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	private String longitude_property_name;
	private String latitude_property_name;
	
	public long Neo4jTime;
	public long JudgeTime;
	
	public Traversal(String suffix, int ratio)
	{
		longitude_property_name = "longitude_"+ratio+"_"+suffix;
		latitude_property_name = "latitude_"+ratio+"_"+suffix;
		Neo4jTime = 0;
		JudgeTime = 0;
	}
	
	public void Preprocess()
	{
		
	}
	
	public boolean ReachabilityQuery(int start_id, MyRectangle rect)
	{
		Queue<Integer> queue = new LinkedList<Integer>();
		VisitedVertices.clear();
		
		long start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		String result = p_neo4j_graph_store.Execute(query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");

		Neo4jTime += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		
		for(int i = 0;i<jsonArr.size();i++)
		{			
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			
			jsonObject = (JsonObject)row.get(1);
			if(jsonObject.has(longitude_property_name))
			{
				double lat = Double.parseDouble(jsonObject.get(latitude_property_name).toString());
				double lon = Double.parseDouble(jsonObject.get(longitude_property_name).toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					JudgeTime+=System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			if(!VisitedVertices.contains(id))
			{
				VisitedVertices.add(id);
				queue.add(id);
			}
		}
		
		JudgeTime += System.currentTimeMillis() - start;
		
		while(!queue.isEmpty())
		{
			start = System.currentTimeMillis();
			
			int id = queue.poll();
			
			query = "match (a)-->(b) where id(a) = " +Integer.toString(id) +" return id(b), b";
			
			result = p_neo4j_graph_store.Execute(query);
			
			jsonParser = new JsonParser();
			jsonObject = (JsonObject) jsonParser.parse(result);
			
			jsonArr = (JsonArray) jsonObject.get("results");
			jsonObject = (JsonObject) jsonArr.get(0);
			jsonArr = (JsonArray) jsonObject.get("data");
			
			Neo4jTime += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			
			for(int i = 0;i<jsonArr.size();i++)
			{			
				jsonObject = (JsonObject)jsonArr.get(i);
				JsonArray row = (JsonArray)jsonObject.get("row");
				
				int neighbor_id = row.get(0).getAsInt();
				
				jsonObject = (JsonObject)row.get(1);
				if(jsonObject.has(longitude_property_name))
				{
					double lat = Double.parseDouble(jsonObject.get(latitude_property_name).toString());
					double lon = Double.parseDouble(jsonObject.get(longitude_property_name).toString());
					if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
					{
						JudgeTime+=System.currentTimeMillis() - start;
						System.out.println(neighbor_id);
						return true;
					}
				}				
				if(!VisitedVertices.contains(neighbor_id))
				{
					VisitedVertices.add(neighbor_id);
					queue.add(neighbor_id);
				}
			}
			JudgeTime += System.currentTimeMillis() - start;
		}	
		return false;
	}
	
}