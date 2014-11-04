import java.util.Scanner;

import main.java.api.AmazonRDSAdapter;
import main.java.api.Database;
import main.java.api.GoogleDatastoreAdapter;
import main.java.api.MongoDBAdapter;
import main.java.api.SimpleDBAdapter;

public class MyDB {
private Scanner scan= new Scanner(System.in);
	public Database connection(int choice){
		Database db=null;
		switch(choice){
		case 1:{db=GoogleDatastore(); break;}   
		case 2:{ db=SimpleDB(); break;}
		case 3:{db=MongoDB(); break;}
		case 4: {db=AmazonRDS(); break;}
		}
		return db;
	}
	//==============================================================================================		
	private	Database GoogleDatastore(){
			//-------- connect---------
		Database db=null;
			System.out.println("To Connect to Google Datastore, Enter the datasetId: ");
			String datasetId = scan.next();
			db=new GoogleDatastoreAdapter(datasetId);
			db.connect();
			return db;
		}
	//==============================================================================================
	private	Database SimpleDB(){
		//------------- connect------
	Database db=null;
		System.out.println("..To Connect to SimpleDB.. ");
		System.out.println("Enter the accessKey: ");
		 String accessKey =  scan.next();
		 System.out.println("Enter the secretKey: ");
		 String secretKey = scan.next();
		 System.out.println("Enter the  region name: ");
		 String region= scan.next();
		 
		 db=new SimpleDBAdapter(accessKey,secretKey,region);
		 db.connect();
	
	return db;
	}
	//==============================================================================================
	private	Database MongoDB(){
		//------------- connect------
	Database db=null;
	System.out.println("..To Connect to MongoDB.. ");
	System.out.println("Enter ClientURL e.g mongodb://[username:password@]host1:port1/database: ");
	String clientURL =scan.next();
	db=new MongoDBAdapter(clientURL);
	db.connect();
	return db;
	}
	//==============================================================================================
	private	Database AmazonRDS(){
		//------------- connect------
	Database db=null;
	
	System.out.println("..To Connect to Amazon RDS.. ");
	System.out.println("Enter the accessKey: ");
	 String accessKey = scan.next();
	 System.out.println("Enter the secretKey: ");
	 String secretKey = scan.next();
	 System.out.println("Enter the  region name: ");
	 String region= scan.next();
	 System.out.println("Enter the endPoint: ");
	 String endPoint = scan.next();
	 System.out.println("Enter the jdbcUrl: ");
	String jdbcUrl = scan.next();
	
    db =new AmazonRDSAdapter(accessKey,secretKey,region,endPoint,jdbcUrl);
	db.connect();
	return db;
	}
}
