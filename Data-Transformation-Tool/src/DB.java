

/**
 * 
 * DB interface has three methods: connect, getData, upload.
 * <p> All databases that supported by this tool need to implement this interface.</p>
 * 
 * */
public interface DB {
	
public void connect();
public void getData(String type);
public void upload(String fileName);

}
