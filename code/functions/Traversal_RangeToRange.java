package functions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Traversal_RangeToRange {
	//used in query procedure in order to record visited vertices
	public Set<Integer> VisitedVertices = new HashSet<Integer>();

	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	private int node_count;
	private String RTreeName;
	private String suffix;
	private Connection con;
	private Statement st;
	private ResultSet rs;

	private String longitude_property_name;
	private String latitude_property_name;

	public long Neo4jTime;
	public long JudgeTime;

	public Traversal_RangeToRange(String suffix, int ratio, String p_datasource)
	{
		longitude_property_name = "longitude_"+ratio+"_"+suffix;
		latitude_property_name = "latitude_"+ratio+"_"+suffix;
		node_count = OwnMethods.GetNodeCount(p_datasource);
		
		if(suffix.equals("random")||suffix.equals("Random"))
			this.RTreeName = p_datasource + "_random_"+ratio;
		else
			this.RTreeName = p_datasource + "_random_"+ratio+"_"+suffix;
		
		con = PostgresJDBC.GetConnection();
		try {
			st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Neo4jTime = 0;
		JudgeTime = 0;
	}

	private void RangeQuery(MyRectangle rect)
	{
		try
		{			
			String query = "select id from " + RTreeName + " where location <@ box '((" + rect.min_x + "," + rect.min_y + ")," + "(" + rect.max_x + "," + rect.max_y + "))'";
			rs = st.executeQuery(query);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void CloseResultSet()
	{
		PostgresJDBC.Close(rs);
	}
	
	public void Disconnect()
	{
		PostgresJDBC.Close(rs);
		PostgresJDBC.Close(st);
		PostgresJDBC.Close(con);
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

	public boolean RangeToRange(MyRectangle start_rect, MyRectangle end_rect)
	{
		try
		{
//			if(start_rect.Area()<end_rect.Area())
			{
				this.RangeQuery(start_rect);
				if(!rs.next())
					return false;
				else
					rs.previous();
				
				Queue<Integer> queue = new LinkedList<Integer>();
				VisitedVertices.clear();
				
				while(rs.next())
				{
					int start_id = rs.getInt("id");
					start_id+=node_count;

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
							if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, end_rect))
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
								if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, end_rect))
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
				}
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
}
