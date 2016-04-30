package experiment;

import java.util.ArrayList;
import java.util.Random;

import functions.Config;
import functions.MyRectangle;
import functions.Neo4j_Graph_Store;
import functions.OwnMethods;
import functions.PostgresJDBC;
import functions.Spatial_Reach_Index;
import functions.Traversal;
import functions.Traversal_RangeToRange;

public class Experiment_RangeToRange {
	

//	public static void Experiment()
//	{
//		String suffix = "random";
//		int ratio = 40;
//		String datasource = "Patents";
//		
////		MyRectangle start_rect = new MyRectangle(29.3, 661, 29.4, 661.4);
////		MyRectangle end_rect= new MyRectangle(223.6, 210.0, 223.7, 210.1);
//		
//		MyRectangle start_rect = new MyRectangle(10, 20, 30, 30);
//		MyRectangle end_rect= new MyRectangle(10, 20, 30, 30);
//		
//		
//		Traversal_RangeToRange tra = new Traversal_RangeToRange(suffix, ratio, datasource);
//		
//		System.out.println(tra.RangeToRange(start_rect, end_rect));
//		tra.Disconnect();
//		
//		String R_Tree_suffix = "";
//		if(suffix.equals("random"))
//			R_Tree_suffix = "";
//		else
//			R_Tree_suffix  = "_"+suffix;
//		Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio , R_Tree_suffix);
//
//		System.out.println(spareach.RangeToRange(start_rect, end_rect));
////		spareach.RangeQuery(start_rect);
//		spareach.Disconnect();
//	}
	
	public static void Experiment_EqualRange()
	{
		// TODO Auto-generated method stub
		MyRectangle rect = new MyRectangle(0,0,1000,1000);
		ArrayList<String> datasource_a = new ArrayList<String>();
		//			datasource_a.add("citeseerx");
					datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
					datasource_a.add("uniprotenc_150m");

					//String suffix = args[0];
		ArrayList<String> suffix_al = new ArrayList<String>();
		//suffix_al.add("random");
		suffix_al.add("clustered");
		suffix_al.add("zipf");

		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = datasource_a.get(name_index);
			
			for(int suffix_index = 0;suffix_index<suffix_al.size();suffix_index++)
			{
				String suffix = suffix_al.get(suffix_index);
				String resultpath = "/home/yuhansun/Documents/Real_data/RangeToRange_query_time_4_27_"+suffix+".csv";
				int node_count = OwnMethods.GetNodeCount(datasource);
				OwnMethods.WriteFile(resultpath, true, datasource+"\n");
				{
					for(int ratio = 20;ratio<=80;ratio+=20)
					{
						OwnMethods.WriteFile(resultpath, true, ratio+"\n");
						OwnMethods.WriteFile(resultpath, true, "spatial_range\ttra_time\ttra_accesscount\ttrue_count\tspareach_time\n");
						long graph_size = OwnMethods.GetNodeCount(datasource);

						ArrayList<Double> a_x_start = new ArrayList<Double>();
						ArrayList<Double> a_y_start = new ArrayList<Double>();
						
						ArrayList<Double> a_x_end = new ArrayList<Double>();
						ArrayList<Double> a_y_end = new ArrayList<Double>();
						

						Random r = new Random();

						int experiment_count_tra = 50;
						int experiment_count_spa = 10;
						double selectivity = 0.0001;
						
						if(datasource.equals("uniprotenc_150m"))
							selectivity = 0.00001;
						
						double spatial_total_range = 1000;
						boolean isrun = true;
						boolean isbreak = false;
						{
							//while(selectivity<=0.02)
							for(int loop = 0;loop<4;loop++)
							{
								double rect_size = spatial_total_range * Math.sqrt(selectivity);
								OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

								a_x_start.clear();
								a_y_start.clear();
								
								a_x_end.clear();
								a_y_end.clear();
								
								for(int i = 0;i<experiment_count_tra;i++)
								{
									a_x_start.add(r.nextDouble()*(1000-rect_size));
									a_y_start.add(r.nextDouble()*(1000-rect_size));
									
									a_x_end.add(r.nextDouble()*(1000-rect_size));
									a_y_end.add(r.nextDouble()*(1000-rect_size));
								}

								int true_count = 0;
								ArrayList<Boolean> tra_result = new ArrayList<Boolean>(); 
								ArrayList<Boolean> spa_result = new ArrayList<Boolean>();	
								//Traversal
								System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
								System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));

								try {
									Thread.currentThread().sleep(3000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}

								Traversal_RangeToRange tra = new Traversal_RangeToRange(suffix, ratio, datasource);

								try {
									Thread.currentThread().sleep(5000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}

								int accessnodecount = 0;
								int time_tra = 0;
								for(int i = 0;i<experiment_count_tra;i++)
								{
									double x = a_x_start.get(i);
									double y = a_y_start.get(i);
									MyRectangle start_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
									
									x = a_x_end.get(i);
									y = a_y_end.get(i);
									MyRectangle end_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);

									System.out.println(i);
									

									try
									{
										tra.VisitedVertices.clear();
										long start = System.currentTimeMillis();
										boolean result1 = tra.RangeToRange(start_rect, end_rect);
										time_tra += System.currentTimeMillis() - start;
										System.out.println(result1);
										tra_result.add(result1);
										if(result1)
											true_count++;
										accessnodecount+=tra.VisitedVertices.size();
									}
									catch(Exception e)
									{
										e.printStackTrace();
										OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
										i = i-1;
									}						
								}
								OwnMethods.WriteFile(resultpath, true, time_tra/experiment_count_tra+"\t"+accessnodecount+"\t"+true_count+"\t");

								//SpaReach
								if(isrun)
								{
									int time_spareach = 0;
									accessnodecount = 0;
									System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
									System.out.println(PostgresJDBC.StopServer());
									System.out.println(OwnMethods.ClearCache());
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									System.out.println(PostgresJDBC.StartServer());
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									String R_Tree_suffix = "";
									if(suffix.equals("random"))
										R_Tree_suffix = "";
									else R_Tree_suffix = "_"+suffix;
									Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio, R_Tree_suffix);
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									for(int i = 0;i<experiment_count_spa;i++)
									{
										double x = a_x_start.get(i);
										double y = a_y_start.get(i);
										MyRectangle start_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
										
										x = a_x_end.get(i);
										y = a_y_end.get(i);
										MyRectangle end_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);

										try
										{
											long start = System.currentTimeMillis();
											boolean result2 = spareach.RangeToRange(start_rect, end_rect);
											time_spareach += (System.currentTimeMillis() - start);
											System.out.println(i);
											System.out.println(result2);
											spa_result.add(result2);
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									spareach.Disconnect();

									for(int i = 0;i<experiment_count_spa;i++)
									{
										if(tra_result.get(i) == spa_result.get(i))
											continue;
										isbreak = true;
										break;
									}

									if(isbreak)
									{
										System.out.println("Inconsistency");
										break;
									}
										
									
									int average_time = time_spareach/experiment_count_spa;
									OwnMethods.WriteFile(resultpath, true, average_time+"\n");
									
									if(average_time>20000)
										experiment_count_spa=3;
//										isrun = false;
									if(average_time>40000)
										experiment_count_spa = 1;
								}
//								else
//									OwnMethods.WriteFile(resultpath, true, "\t");

								selectivity*=10;
							}
						}
						OwnMethods.WriteFile(resultpath, true, "\n");
					}
				}


				OwnMethods.WriteFile(resultpath, true, "\n");
				System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
			}
			
		}	

	}
	
	public static void main(String[] args) {
		Experiment_EqualRange();
	}

}
