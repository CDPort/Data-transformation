import java.util.Scanner;


public class Main {
    public static void main(String[] args) throws Exception {
    	DB FromDB= null;
    	DB ToDB= null;
    	
		Scanner scan = new Scanner(System.in);
		System.out.println("Select the current cloud storage system to get the data: 1- Google Datastore 2-SimpleDB 3-MongoDB ");
		int from=scan.nextInt();
		System.out.println("Select the new cloud storage system to upload the Data: 1- Google Datastore 2-SimpleDB 3-MongoDB ");
		int to=scan.nextInt();
//----------------From----------------
			if(from==1)      FromDB= new MyDatastore();
			else if(from==2) FromDB= new MySimpleDB();
			else if(from==3) FromDB= new MyMongoDB();

			//1- connect
			FromDB.connect();
			//2- retrieve data
			System.out.println("..To retrive the data..\n Enter the Entity_Type: ");
			String type = scan.next();
			 FromDB.getData(type);
		
			 
//----------------To-----------------
			if(to==1)     ToDB= new MyDatastore();
			else if(to==2)ToDB= new MySimpleDB();
			else if(to==3)ToDB=new MyMongoDB();
		
			//1- connect
			ToDB.connect();
			//2- upload the data from the XML file
			System.out.println(" Enter the file name  that contains the data to upload them (e.g. filename.xml): "); 
			String fileName= scan.next();
	  	
			ToDB.upload(fileName);

    }//end main  


}