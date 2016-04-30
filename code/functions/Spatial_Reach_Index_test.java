package functions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class Spatial_Reach_Index_test {
	
	public static ArrayList<String> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/experiment_id.txt";
		ArrayList<String> al = new ArrayList<String>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(temp);
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
		}
		return al;
	}
	
	public static void Experiment()
	{
//		MyRectangle rect = new MyRectangle(0,0,1000,1000);
//		Config p_con = new Config();
//		String suffix = p_con.GetSuffix();
//		ArrayList<String> datasource_a = new ArrayList<String>();
////		datasource_a.add("citeseerx");
////		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
////		datasource_a.add("uniprotenc_22m");
////		datasource_a.add("uniprotenc_100m");
////		datasource_a.add("uniprotenc_150m");
//		for(int name_index = 0;name_index<datasource_a.size();name_index++)
//		{
//			String datasource = datasource_a.get(name_index);
//			String resultpath = "/home/yuhansun/Documents/Real_data/"+datasource+"/SpaReach"+suffix+".csv";
////			String resultpath = "/home/yuhansun/Documents/Real_data/multi_test"+suffix+".csv";
//			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
//			int pieces = 128;
//			{
//				for(int ratio = 20;ratio<=20;ratio+=20)
//				{
//					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
//					String graph_label = "Graph_Random_" + ratio;
//					long graph_size = OwnMethods.GetNodeCount(datasource);
//					int offset = (int) (ratio/20*graph_size);
////					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//					long experiment_node_count = 500;
//					int start_index = 0;
////					HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
////					ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
////					System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//					
//					ArrayList<String> temp_al = ReadExperimentNode(datasource);
//					ArrayList<String> al = new ArrayList<String>();
//					for(int i = 0;i<temp_al.size();i++)
//					{
//						Integer absolute_id = Integer.parseInt(temp_al.get(i))+offset;
//						String x = absolute_id.toString();
//						al.add(x);
//					}
//									
//					ArrayList<Double> a_x = new ArrayList<Double>();
//					ArrayList<Double> a_y = new ArrayList<Double>();
//					
//					Random r = new Random();
//					
//					double selectivity = 0.1;
//					double spatial_total_range = 1000;
//					boolean isbreak = false;
//					boolean isrun = true;
//					{
//						{
//							double rect_size = spatial_total_range * Math.sqrt(selectivity);
//							OwnMethods.WriteFile(resultpath, true, selectivity+"\n");
//							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/record.txt", true, selectivity+"\n");
//							
//							a_x.clear();
//							a_y.clear();
//							for(int i = 0;i<experiment_node_count;i++)
//							{
//								a_x.add(r.nextDouble()*(1000-rect_size));
//								a_y.add(r.nextDouble()*(1000-rect_size));
//							}
//							
//							int true_count = 0;
//							ArrayList<Boolean> geo_RMBR_result = new ArrayList<Boolean>();
//							ArrayList<Boolean> geo_full_result = new ArrayList<Boolean>();
//							ArrayList<Boolean> geo_multilevel2_result = new ArrayList<Boolean>();
//							ArrayList<Boolean> geo_multilevel3_result = new ArrayList<Boolean>();
//							ArrayList<Boolean> geo_partial_result = new ArrayList<Boolean>();
//							ArrayList<Boolean> spareach_result = new ArrayList<Boolean>();
//								
//								
////								//SpaReach
//								if(isrun)
//								{
//									int time_spareach = 0;
//									System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//									System.out.println(PostgresJDBC.StopServer());
//									System.out.println(OwnMethods.ClearCache());
//									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//									System.out.println(PostgresJDBC.StartServer());
//									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//									try {
//										Thread.currentThread().sleep(5000);
//									} catch (InterruptedException e1) {
//										// TODO Auto-generated catch block
//										e1.printStackTrace();
//									}
//									Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio);
//									try {
//										Thread.currentThread().sleep(5000);
//									} catch (InterruptedException e1) {
//										// TODO Auto-generated catch block
//										e1.printStackTrace();
//									}
//									for(int i = 11;i<al.size();i++)
//									{
//										double x = a_x.get(i);
//										double y = a_y.get(i);
//										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
//										
//										System.out.println(i);
//										int id = Integer.parseInt(al.get(i)) - offset;
//										System.out.println(id);
//										
//										try
//										{
//											long start = System.currentTimeMillis();
//											boolean result2 = spareach.ReachabilityQuery(id, query_rect);
//											long time = System.currentTimeMillis() - start;
//											time_spareach += time;
//											OwnMethods.WriteFile(resultpath, true, time+"\t"+spareach.neo4j_time+"\t"+spareach.postgresql_time+"\t"+spareach.judge_time+"\t"+result2+"\n");
//											spareach.neo4j_time = 0;
//											spareach.postgresql_time = 0;
//											spareach.judge_time = 0;
//											
//										}
//										catch(Exception e)
//										{
//											e.printStackTrace();
//											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//											i = i-1;
//										}						
//									}
//									spareach.Disconnect();
//									if(time_spareach/experiment_node_count>10000)
//										isrun = false;
//								}
//								
//							for(int i = 0;i<experiment_node_count;i++)
//								if(spareach_result.get(i))
//									true_count++;
//							
//							break;
//						}
//					}
//				}
//			}
//			
//			
//			OwnMethods.WriteFile(resultpath, true, "\n");
//			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//		}		
		
//		String datasource = "Patents";
//		String suffix = "random";
//		int ratio = 20;
//		int id = 6;
//		int node_count = OwnMethods.GetNodeCount(datasource);
//		Traversal tra = new Traversal(suffix, ratio);
//		Spatial_Reach_Index spa = new Spatial_Reach_Index(datasource+"_random_"+ratio, "");
//		MyRectangle rect = new MyRectangle(200,200,250,250);
//		System.out.println(tra.ReachabilityQuery(id+node_count, rect));
//		System.out.println(spa.ReachabilityQuery(id, rect));
//		spa.Disconnect();
		
		{
			MyRectangle rect = new MyRectangle(0,0,1000,1000);
			Config p_con = new Config();
			ArrayList<String> datasource_a = new ArrayList<String>();
//			datasource_a.add("citeseerx");
//			datasource_a.add("go_uniprot");
			datasource_a.add("Patents");
//			datasource_a.add("uniprotenc_150m");
			
//			String suffix = args[0];
			String suffix = "random";
			
			for(int name_index = 0;name_index<datasource_a.size();name_index++)
			{
				String datasource = datasource_a.get(name_index);
				String resultpath = "/home/yuhansun/Documents/Real_data/query_time_4_25_"+suffix+".csv";
				int node_count = OwnMethods.GetNodeCount(datasource);
				OwnMethods.WriteFile(resultpath, true, datasource+"\n");
				{
					for(int ratio = 20;ratio<=20;ratio+=20)
					{
						OwnMethods.WriteFile(resultpath, true, ratio+"\n");
						OwnMethods.WriteFile(resultpath, true, "spatial_range\ttra_time\tspareach_time\ttrue_count\taccess_count\n");
						
						ArrayList<String> tmp_al = ReadExperimentNode(datasource);
						ArrayList<String> experiment_id_al = new ArrayList<String>();
						for(int i = 0;i<tmp_al.size();i++)
						{
							Integer absolute_id = Integer.parseInt(tmp_al.get(i))+node_count;
							String x = absolute_id.toString();
							experiment_id_al.add(x);
						}
										
						ArrayList<Double> a_x = new ArrayList<Double>();
						ArrayList<Double> a_y = new ArrayList<Double>();
						
						Random r = new Random();
						
						double selectivity = 0.001;
						double spatial_total_range = 1000;
						boolean isrun = true;
						boolean isbreak = false;
						{
							while(selectivity<=1)
							{
								double rect_size = spatial_total_range * Math.sqrt(selectivity);
								OwnMethods.WriteFile(resultpath, true, selectivity+"\t");
								
								a_x.clear();
								a_y.clear();
								for(int i = 0;i<experiment_id_al.size();i++)
								{
									a_x.add(r.nextDouble()*(1000-rect_size));
									a_y.add(r.nextDouble()*(1000-rect_size));
								}
								
								int true_count = 0;
								//Traversal
								Traversal tra = new Traversal(suffix, ratio);
																
								//SpaReach
								if(isrun)
								{
									int time_spareach = 0;
									
									String R_Tree_suffix = "";
									if(suffix.equals("random"))
										R_Tree_suffix = "";
									else
										R_Tree_suffix  = "_"+suffix;
									Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio , R_Tree_suffix);

									for(int i = 0;i<experiment_id_al.size();i++)
									{
										double x = a_x.get(i);
										double y = a_y.get(i);
										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);

										System.out.println(i);
										int id = Integer.parseInt(experiment_id_al.get(i));
										System.out.println(id);

										try
										{
											long start = System.currentTimeMillis();
											System.out.println(id - node_count);
											boolean result2 = spareach.ReachabilityQuery(id - node_count, query_rect);
											time_spareach += (System.currentTimeMillis() - start);
											System.out.println(result2);
											
											tra.VisitedVertices.clear();
											start = System.currentTimeMillis();
											boolean result3 = tra.ReachabilityQuery(id, query_rect);
											System.out.println(result3);
											if(result3)
												true_count++;
											
											if(result2!=result3)
											{
												isbreak = true;
												break;
											}
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									spareach.Disconnect();
									

									if(isbreak)
										break;
									OwnMethods.WriteFile(resultpath, true, time_spareach/(tmp_al.size())+"\t");
								}
								else
									OwnMethods.WriteFile(resultpath, true, "\t");

								selectivity*=10;
								System.out.println(selectivity);
							}
						}
						OwnMethods.WriteFile(resultpath, true, "\n");
					}
				}
				
				
				OwnMethods.WriteFile(resultpath, true, "\n");
			}		
		}
		
	}

	public static void main(String[] args) throws SQLException {
		
		Experiment();
		// TODO Auto-generated method stub
//		String datasource = "Patents";
//		Neo4j_Graph_Store.StartMyServer(datasource);
//
//		MyRectangle range = new MyRectangle(0,0,1000,1000);
//		
//		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 128);
//		Spatial_Reach_Index spareach = new Spatial_Reach_Index("Patents_Random_20");
//
////		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 10);
////		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 5);
//		
////		double x = 871.0048749159089;
////		double y = 185.37284713093848;
////		MyRectangle rect = new MyRectangle(x, y, x+100, y+100); 
////		System.out.println(p_geo.ReachabilityQuery(5479369, rect));
//		
//		//int graph_size = OwnMethods.GetNodeCount(datasource);
//		int graph_size = 3774768;
//		double experiment_node_count = 500;
//		String graph_label = "Graph_Random_20";
//		double spatial_total_range = 1000;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		
//		Random r = new Random();
//		HashMap<Integer, ArrayList<Double>> hs_x = new HashMap();
//		HashMap<Integer, ArrayList<Double>> hs_y = new HashMap();
//		for(int j = 1;j<100;j*=10)
//		{
//			ArrayList<Double> lx = new ArrayList();
//			ArrayList<Double> ly = new ArrayList();
//			double rect_size = spatial_total_range * Math.sqrt(j/10000.0);
//			for(int i = 0;i<experiment_node_count;i++)
//			{
//				double x = r.nextDouble()*(spatial_total_range-rect_size);
//				double y = r.nextDouble()*(spatial_total_range-rect_size);
//				lx.add(x);
//				ly.add(y);
//			}
//			hs_x.put(j, lx);
//			hs_y.put(j, ly);
//		}
//		
//		boolean isbreak = false;
//		int true_count = 0;
//		for(int j = 1;j<100;j*=10)
//		{
//			double rect_size = spatial_total_range * Math.sqrt(j/10000.0);
//			System.out.println(rect_size);
//			ArrayList<Double> lx = hs_x.get(j);
//			ArrayList<Double> ly = hs_y.get(j);
//			
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				double x = lx.get(i);
//				double y = ly.get(i);
//				MyRectangle query_rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
//				
//				boolean result1 = spareach.ReachabilityQuery(id, query_rect);
////				System.out.println(result1);
//				boolean result2 = p_geo.ReachabilityQuery_FullGrids(id, query_rect);
////				System.out.println(result2);
//				
//				if(result1)
//					true_count+=1;
//				
//				if(result1!=result2)
//				{
//					System.out.println(i);
//					System.out.println(x);
//					System.out.println(y);
//					System.out.println(rect_size);
//					System.out.println(true_count);
//					
//					isbreak = true;
//					break;
//				}
//			}
//			
//			System.out.println(true_count);
//			
//			if(isbreak)
//			{
//				System.out.println("error");
//				break;
//			}
//		}
//		spareach.Disconnect();
//		Neo4j_Graph_Store.StopMyServer(datasource);
	}
}
