import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.bson.types.ObjectId;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import main.java.api.Database;
import main.java.api.Entities;
import main.java.api.EntityKey;
import main.java.api.MongoDBAdapter;
import main.java.api.Properties;
import main.java.api.PropertyValue;
import main.java.api.Response;

/**
 * MyMongoDB class  implements the interface DB.
 * <p> IT has the following functions: 
 * <li> Connect: enable connection to MongoDB, </li>
 * <li> getDate: to retrieve all entities belong to the specified entity Type, the retrieved data are stored in XML file after converted into CDPort's data model, </li>
 * <li> upload: to upload the data from XML file to MongoDB.
 */

public class MyMongoDB  implements DB{
	private Database db=null;
	Scanner scan = new Scanner(System.in);
	/**
	 * Connection
	 * 
	 * <p> need user gives the ClientURL. <p/>
	 * 
	 */

	@Override
	public void connect() {
		//------------------------(1) connect
		System.out.println("..To Connect to MongoDB.. ");
		System.out.println("Enter ClientURL e.g mongodb://[username:password@]host1:port1/database: ");
		String clientURL =scan.next();
	    db=new MongoDBAdapter(clientURL);
		db.connect();
		
	}
	/**
	 * Get the data from the Cloud storage, then convert it to the CDPort's Data mode.
	 * <p> save  the retrieved data  in  XML file </p>
	 * @param type the entity Type (In MongoDB, it is called the 'Collection' name).
	 */
	@Override 
	public void getData(String type){
	//----------------------create query and select keys of all entities belongs to the given Type-------------------  
		String [] selectExpression = {"select", "KEY", "from", type} ;
		Response resp=	db.query(selectExpression);
	//----------------------to get properties of each entity, and then store it in xml file--------------
		System.out.println("Start downloading all entities in "+type +" ..");
	//-- variables to store in xml--
		  Element prop, ElmBeg, entityElm;
		Attr  key, keyDataType;							 
			try {
				DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
				 DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
				 // root elements-------------------------------
				 Document doc= docBuilder.newDocument();
				Element rootElement = doc.createElement("Exported");
				doc.appendChild(rootElement);
				 //-- check if the query result is not empty --
			   if(resp.getEntities().size()>0){
					 //<EntityType> 
					  entityElm = doc.createElement("EntityType");
					  String entityType =resp.getEntities().get(1).getEntityType();
					  entityElm.appendChild( doc.createTextNode(entityType));
					  rootElement.appendChild(entityElm);
					//--------loop to get keys (the result of the query) ------
				 for (Entities entity : resp.getEntities()) {
				 //---------------set entity_key ---------------
					   //  <entity>-------------------
					   ElmBeg = doc.createElement("Entity");
					   rootElement.appendChild(ElmBeg);
					  //  <Name>-------------------
					    key = doc.createAttribute("Key");
					   	keyDataType=doc.createAttribute("keyDataType");	        		
					    EntityKey entityKey=new EntityKey();

				//*********check _key_ data type****************
				     if(entity.getKey().hasIntkey())
			    		 {key.setValue(entity.getKey().getIntkey()+"");
			    		 keyDataType.setValue("int");
			    		 entityKey.setIntkey(entity.getKey().getIntkey());
			    		 }
				    	else if(entity.getKey().hasObjectIdkey())
			    		{key.setValue(entity.getKey().getObjectIdkey()+"");
			    		keyDataType.setValue("ObjectId");
			    		 entityKey.setObjectIdkey(entity.getKey().getObjectIdkey());
			    		}
				    	else 
	    		        	{key.setValue(entity.getKey().getStringkey());
    		        		keyDataType.setValue("String");
    		        		entityKey.setStringkey(entity.getKey().getStringkey());
    		        		}
				     ElmBeg.setAttributeNode(key);
				     ElmBeg.setAttributeNode(keyDataType);
				//------------------Get the properties---------------
				     Response property=  db.getProperty(type, entityKey);
			    	//  <Properties> -----------------
				   prop = doc.createElement("Properties");
				  ElmBeg.appendChild(prop);		               
				 for(int i=0; i<property.getProperties().size();i++)//get all properties 
			        { Element PropName, PropValue;
		              Attr valueDataType;
			        	String ProbName=  property.getProperties().get(i).getPropertyName();
			            //<name> property--------------
			        	 PropName= doc.createElement("Name");
			             PropName.appendChild(doc.createTextNode(ProbName));
			             prop.appendChild(PropName);
			        PropertyValue value=	property.getProperties().get(i).getPropertyValue();      
			        	//<value> property--------------------
			        	PropValue= doc.createElement("Value");
		                  valueDataType=doc.createAttribute("valueDataType");	
		      //****************************check type*********************************************
		     //*********************
		                  if(value.hasArrayValue())
 			        	{
 			        	int y=0;
 			        	while(y<value.getArray().length)
 			        	{Element PropArrayValue;
 			        	PropArrayValue= doc.createElement("ArrayValues");
 			        		if(value.getArray()[y].haslongValue())
 			        		{PropArrayValue.appendChild(doc.createTextNode(value.getArray()[y].getLong()+"")); 
 			        		valueDataType.setValue("long_Array");
 			        		}
 			        		else if(value.getArray()[y].hasDateValue())
 			        		{PropArrayValue.appendChild(doc.createTextNode(value.getArray()[y].getDate()+"")); 
 			        		valueDataType.setValue("Date_Array");
 			        		}
 			        		else 
 			        		{
 			        			PropArrayValue.appendChild(doc.createTextNode(value.getArray()[y].getString()+"")); 
	    			        	valueDataType.setValue("String_Array");
 			        		}
 			        		PropValue.appendChild(PropArrayValue);
 			        		 y++;}
			                }//end if
		                  else// if not array
		                {
			          if(value.haslongValue())
			        	{PropValue.appendChild(doc.createTextNode(value.getLong()+"")); 
		        		valueDataType.setValue("long");
		                }
			        	else if(value.hasDateValue())
			        	{PropValue.appendChild(doc.createTextNode(value.getDate()+"")); 
		        		valueDataType.setValue("Date");
		                }
			        	else //string
			        	 {
			                PropValue.appendChild(doc.createTextNode(value.getString())); 
			        		valueDataType.setValue("String");
			              }
		                } 
		     //***********************
			        PropValue.setAttributeNode(valueDataType);
			        prop.appendChild(PropValue);
			   	 }// end for2
					}// end for 1
		      	//------ write the content into xml file---------
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		// ********************************save the retrieved data in XML file *******************************
		System.out.println(" Enter the file name to save the retrieved data (e.g. filename.xml): "); 
		String fileName= scan.next();
  		StreamResult result = new StreamResult(new File(fileName));

		// Output to console for testing
		//StreamResult result = new StreamResult(System.out);

		transformer.transform(source, result);	
		System.out.println("\n File saved! \n");
	}else 	System.out.println("Error:no data retrieved");
			
		} catch (ParserConfigurationException pce) {
		pce.printStackTrace();
		} catch (TransformerException tfe) {
		tfe.printStackTrace();
		}

}
	/**
	 * Upload the data from XML file to the Cloud storage.
	 * <p> The data in the XML file can be from any of the supported cloud storage systems. </p>
	 * @param fileName the current XML file that has the data.
	 */
	@Override
	public void upload(String fileName){
		System.out.println("Start uploading all entities from "+fileName +" ..");
		try{
			Document doc2= null;
			 String xml="";
			  try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			        StringBuilder sb = new StringBuilder();
			        String line = br.readLine();
			        // read data from xml
			        while (line != null) {
			            sb.append(line);
			            sb.append(System.lineSeparator());
			            line = br.readLine();
			        } // convert from StringBuilder to String
			         xml = sb.toString();
			    }

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder doc = dbf.newDocumentBuilder();
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(xml));
		   doc2 =doc.parse(is); 
		    //--get entity type--
		 	NodeList typeNode =doc2.getElementsByTagName("EntityType");
		 	String type = typeNode.item(0).getTextContent();
		 	
		 	NodeList nList=  doc2.getElementsByTagName("Entity");
	    //--get all nodes with tag 'entity'  --
	for (int temp = 0; temp < nList.getLength(); temp++) {
		//--get all nodes in entity element--
		Node nNode = nList.item(temp);
		//--get elements--
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				Element eElement = (Element) nNode;
				//____________________set key  ______________________
				EntityKey key= new EntityKey();
				//************Check  _key_ data type**********
				//********
					 if(eElement.getAttribute("keyDataType").equalsIgnoreCase("int"))
					key.setIntkey(Long.parseLong(eElement.getAttribute("Key")));
					 else if(eElement.getAttribute("keyDataType").equalsIgnoreCase("ObjectId"))
					key.setObjectIdkey(new ObjectId(eElement.getAttribute("Key"))); 
					 else 
				    key.setStringkey(eElement.getAttribute("Key"));
					//*********

				//___________________get property name and value ___
					int count=0;
					String propName;
					String propValue;
			   		List<Properties> propList=new ArrayList<Properties>();
				while(eElement.getElementsByTagName("Name").item(count)!=null){
						propName=eElement.getElementsByTagName("Name").item(count).getTextContent();
					   propValue=eElement.getElementsByTagName("Value").item(count).getTextContent();
		   			//System.out.println("Name : " + propName);
		   			//System.out.println("Value : " + propValue);
		   			
			     	//__________ check the value type before set the property______
		   			Properties prop=new Properties();
		   			PropertyValue value=new PropertyValue();
		   			//**************************************Check _property Value_ data type**************************************
						//********************************can be long, string, Date or array
			   			String propType=eElement.getElementsByTagName("Value").item(count).getAttributes().getNamedItem("valueDataType").getTextContent();
						  if(propType.equalsIgnoreCase("long")) 
							  value.setLong(Long.parseLong(propValue));
						  else   if(propType.equalsIgnoreCase("Date")) 
							 {DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							 Date date = dateFormat.parse( propValue);
							 value.setDate(date);
							 }
						 else   if(propType.contains("Array"))
							 { int x=0;
							// String array=eElement.getElementsByTagName("ArrayValues").item(count).getNodeValue();
					    		PropertyValue [] propertyValueArray=new PropertyValue [eElement.getElementsByTagName("ArrayValues").getLength()];;
					    	while(eElement.getElementsByTagName("ArrayValues").item(x)!=null)
					    	{
					    			propertyValueArray[x]=new  PropertyValue();
					    			String v=eElement.getElementsByTagName("ArrayValues").item(x).getTextContent() ;
					    			if (propType.equalsIgnoreCase("long_Array"))propertyValueArray[x].setLong(Long.parseLong(v));
					    			else if (propType.equalsIgnoreCase("Date_Array")) 
					    			{DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
									 Date date = dateFormat.parse(v);
									 propertyValueArray[x].setDate(date);
					    			}
					    			else propertyValueArray[x].setString(v);
					    	x++;}
								 value.setArray(propertyValueArray);

						     }
						else
		   				   value.setString(propValue);
					//***************************************************

		   		prop.setProperity(propName, value);//___set property___
		   		propList.add(prop);
		   		 //___________
		   		count++;}//end while --read all properties--
					//__________________
					//put entity key and type, property list
					db.put(type, key, propList, false);

				}}
	  		System.out.println("\n ... Data uploaded! ... \n");
		 } catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		 } catch (Exception e) {
			e.printStackTrace();
		 }
	}
	}

