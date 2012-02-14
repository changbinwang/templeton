package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hcatalog.templeton.TempletonStorage.Type;

public class HDFSStorage implements TempletonStorage {
	FileSystem fs = null;
	
	public static final String STORAGE_ROOT = "/user/templeton/storage";
    
    public static final String JOB_PATH = STORAGE_ROOT + "/jobs";
    public static final String JOB_TRACKINGPATH = STORAGE_ROOT + "/created";
    public static final String OVERHEAD_PATH = STORAGE_ROOT + "/overhead";

	@Override
	public void saveField(Type type, String id, String key, String val)
			throws NotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getField(Type type, String id, String key) {
		//FSDataInputStream in = fs.open(inFile);
		return null;
	}

	@Override
	public Map<String, String> getFields(Type type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean delete(Type type, String id) throws NotFoundException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> getAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getAllForType(Type type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getAllForKey(String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getAllForTypeAndKey(Type type, String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void openStorage() throws IOException {
		if (fs == null) {
			fs = FileSystem.get(AppConfig.getInstance());
		}
	}

	@Override
	public void closeStorage() throws IOException {
		if (fs != null) {
			fs.close();
		}

	}
	
	/**
	 * Get the path to storage based on the type.
	 * @param type
	 * @return
	 */
	public String getPath(Type type) {
    	String typepath = OVERHEAD_PATH;
    	switch (type) {
    	case JOB:
    		typepath = JOB_PATH;
    		break;
    	case JOBTRACKING:
    		typepath = JOB_TRACKINGPATH;
    		break;
    	}
    	return typepath;
	}

}
