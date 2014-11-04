

	import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

	import main.java.api.Database;
import main.java.api.Entities;
import main.java.api.EntityKey;
import main.java.api.Properties;
import main.java.api.PropertyValue;
import main.java.api.Response;

	public class Main_Transform {
		
private static Database FromDB= null;
private	static Database ToDB= null;
private	static String	eType="";
private	static String	type="";
private	static  int limit=0;

		public static void main(String[] args) throws Exception {	
			Scanner scan = new Scanner(System.in);
			System.out.println("Select the source cloud storage system to get the data: 1- Google Datastore 2-SimpleDB 3-MongoDB  4-AmazonRDS");
			int from=scan.nextInt();
			System.out.println("Select the target storage system to upload the Data: 1- Google Datastore 2-SimpleDB 3-MongoDB  4-AmazonRDS");
			int to=scan.nextInt();
	//.........(1)......... Connect
			MyDB con=new MyDB();
			//-------Source DB---------
			FromDB=con.connection(from);
			//-------Target DB---------
			ToDB=con.connection(to);

	//.........(2).........retrieve data
			System.out.println("..To retrive the data..\n Enter the Entity_Type: ");
			 type = scan.next();
			System.out.println("Enter the name of the target \"Entity_Type\" to insert entities..");
				eType=scan.next(); 
					
			if(eType.isEmpty()||eType.equals("")) eType=type;
					//ToDB.createEntityType(eType);	

	//..............(3).......transform data. 
			transform ();
			//.............(optional) save backup of the source metadata
			System.out.println("Do you want to store metadata of source data..(y/n)");
			String catalog_option=scan.next();
			if(catalog_option.equalsIgnoreCase("y"))
				createCatalog();
	    }//end main  
		
		
		
		
		
		
		
private static void transform (){
	Scanner scan = new Scanner(System.in);

	//System.out.println("Enter the min. number of the entities moved once a time..");
	 limit=500;//scan.nextInt();
	int startCount=0;
	int endCount=0;
	
	//...............get number of entities in source......
	String [] selectExpression = {"select", "count(*)", "from", type} ;
	long startquery=System.nanoTime();
	Response resp=	FromDB.query(selectExpression);
	long endquery=System.nanoTime();
	long sumQuery= endquery-startquery;
	int respSize=(int)Long.parseLong(resp.getEntities().get(0).getProperties().get(0).getPropertyValue().getString());
	
System.out.println("...Start downloading  entities in "+type +" .."+respSize+" entities");
boolean allIsMoved=false;	
	if(limit<respSize)
		endCount=limit ;
	else 
		endCount=respSize;
	
do{
 System.out.println(" .................................................");
 System.out.println("limit="+limit+"\t endCount="+endCount);
 //check 
if(endCount>respSize)
{endCount=respSize;allIsMoved=true;}
//...........get..................
Response entities= new Response ();
System.out.println("\t startCount="+startCount);
entities= FromDB.getEntity(type, startCount,500);//get 500**
int currentEntitiesSize=entities.getEntities().size();
  	//..............put....................
 		int eLimit=5;//********************
 		if(currentEntitiesSize<eLimit)
 			eLimit=currentEntitiesSize;
 		int entIndex=0;
 		do{
	 		List<Entities> fiveEntities=new ArrayList<Entities>();
	 		while(entIndex<eLimit) 
	 		{fiveEntities.add(entities.getEntities().get(entIndex));
	 		entIndex++;
	 		}
			ToDB.put(eType, fiveEntities);
			 System.out.println("....put  .."+eLimit+"total="+endCount);
			 if(eLimit==currentEntitiesSize) break;
			eLimit=eLimit+5;//***************************************
			if(currentEntitiesSize<eLimit)
	 			eLimit=currentEntitiesSize;
 		}while(eLimit<=currentEntitiesSize);

  System.out.println("put  .."+endCount);
	if(endCount==respSize) break;
	startCount=endCount;
endCount+=limit;
}while(!allIsMoved);
	   
	    
System.out.println(" Finish ........."+(endCount-limit));
}






private static void createCatalog (){
	Scanner scan =new Scanner(System.in);
	System.out.println("...Enter the name of the catalog....");
	String catalogName=scan.next();
	//----create query and select keys of all entities belongs to the given Type-------------------  
	String [] selectExpression = {"select", "KEY", "from", type} ;
	Response resp=	FromDB.query(selectExpression);
	System.out.println("select "+resp.getEntities().size());
if(resp.getEntities().size()>0){
	int respSize=resp.getEntities().size();	
	System.out.println("...Start downloading information about entities in "+type +" .."+respSize+" entities");
	//.....get.......
	for(int i=0;i<resp.getEntities().size();i++){
		Response Propresp=	FromDB.getProperty(type, resp.getEntities().get(i).getKey());
		List<Properties> pList=new ArrayList<Properties>();
		String KeyType="";
		EntityKey k=new EntityKey();	
			//....set key type.....
			EntityKey currentkey=resp.getEntities().get(i).getKey();
			if(currentkey.hasIntkey())
				KeyType="long";
			else if(currentkey.hasObjectIdkey())
				KeyType="objectID";
			else if(currentkey.hasStringkey())
				KeyType="string";	
	  k.setStringkey(KeyType);
		//.....set properties types......
	  int index=0;
	  List<Properties> currentProperties=Propresp.getProperties();
	  List<Properties> newPropList=new ArrayList<Properties>();
		 while(index<resp.getProperties().size()){
			 Properties p= new Properties();
			 //..check type of the property value 
			 PropertyValue v=new PropertyValue();
		    	if(currentProperties.get(index).getPropertyValue().haslongValue())
		    		v.setString("long");
		    	else if(currentProperties.get(index).getPropertyValue().hasDoubleValue())
		    		v.setString("double");
		    	else if(currentProperties.get(index).getPropertyValue().hasDateValue())
		    		v.setString("date");
		    	else if(currentProperties.get(index).getPropertyValue().hasArrayValue())	
		    		v.setString("aray");
		    	else
		    		v.setString("string");
		  p.setProperity(currentProperties.get(index).getPropertyName(), v);
		  newPropList.add(p);	   
     index++;
     }
		
	//.............put information about enity.......	
		ToDB.put(catalogName, k, pList,true);
		}
		

}     
}

	}
