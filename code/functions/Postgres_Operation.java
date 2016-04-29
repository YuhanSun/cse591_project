package functions;

import java.util.ArrayList;

public class Postgres_Operation {
	
	public static void CreateTable()
	{
		PostgresJDBC psql = new PostgresJDBC();
		
		ArrayList<String> datasource_al = new ArrayList<String>();
		datasource_al.add("citeseerx");
		datasource_al.add("go_uniprot");
//		datasource_al.add("Patents");
		datasource_al.add("uniprotenc_150m");
		
		ArrayList<String> suffix_al = new ArrayList<String>();
//		suffix_al.add("Random");
		suffix_al.add("Clustered");
		
		
		for(int datasource_index = 0;datasource_index<datasource_al.size();datasource_index++)
		{
			String datasource = datasource_al.get(datasource_index);
			for(int suffix_index = 0;suffix_index<suffix_al.size();suffix_index++)
			{
				String suffix = "";
				String filesuffix = "";
				if(suffix_al.get(suffix_index).equals("Random"))
				{
					suffix = "";
					filesuffix = "Random_spatial_distributed";
				}
				else
				{
					suffix = "_"+suffix_al.get(suffix_index);
					filesuffix =  suffix_al.get(suffix_index)+"_distributed";
				}
				for(int ratio = 20;ratio<=80;ratio+=20)
				{
					psql.CreateTable(datasource, suffix, ratio);
				}
			}
		}
		
		psql.DisConnect();
	}
	
	public static void LoadData()
	{
		PostgresJDBC psql = new PostgresJDBC();
		
		ArrayList<String> datasource_al = new ArrayList<String>();
//		datasource_al.add("citeseerx");
//		datasource_al.add("go_uniprot");
//		datasource_al.add("Patents");
		datasource_al.add("uniprotenc_150m");
		
		ArrayList<String> suffix_al = new ArrayList<String>();
		suffix_al.add("Random");
//		suffix_al.add("Clustered");
		
		
		for(int datasource_index = 0;datasource_index<datasource_al.size();datasource_index++)
		{
			String datasource = datasource_al.get(datasource_index);
			for(int suffix_index = 0;suffix_index<suffix_al.size();suffix_index++)
			{
				String suffix = "";
				String filesuffix = "";
				if(suffix_al.get(suffix_index).equals("Random"))
				{
					suffix = "";
					filesuffix = "Random_spatial_distributed";
				}
				else
				{
					suffix = "_"+suffix_al.get(suffix_index);
					filesuffix =  suffix_al.get(suffix_index)+"_distributed";
				}
				for(int ratio = 40;ratio<=80;ratio+=20)
				{
					psql.LoadData(datasource, suffix, filesuffix, ratio);
				}
			}
		}
		psql.DisConnect();
	}
	
	public static void CreateGistIndex()
	{
		PostgresJDBC psql = new PostgresJDBC();
		
		ArrayList<String> datasource_al = new ArrayList<String>();
//		datasource_al.add("citeseerx");
//		datasource_al.add("go_uniprot");
//		datasource_al.add("Patents");
		datasource_al.add("uniprotenc_150m");
		
		ArrayList<String> suffix_al = new ArrayList<String>();
//		suffix_al.add("Random");
		suffix_al.add("Clustered");
//		suffix_al.add("Zipf");
		
		for(int datasource_index = 0;datasource_index<datasource_al.size();datasource_index++)
		{
			String datasource = datasource_al.get(datasource_index);
			for(int suffix_index = 0;suffix_index<suffix_al.size();suffix_index++)
			{
				String suffix = "";
				if(suffix_al.get(suffix_index).equals("Random"))
				{
					suffix = "";
				}
				else
				{
					suffix = "_"+suffix_al.get(suffix_index);
				}
				for(int ratio = 20;ratio<=80;ratio+=20)
				{
					psql.CreateGistIndex(datasource, suffix, ratio);
				}
			}
		}
		psql.DisConnect();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		CreateTable();
//		LoadData();
		CreateGistIndex();
	}

}
