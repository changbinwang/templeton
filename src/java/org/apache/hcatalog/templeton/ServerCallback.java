package org.apache.hcatalog.templeton;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple callback class that just registers a callback url when a job is started, and
 * calls it back with the jobid when it's finished. 
 *
 */
public class ServerCallback {
	// The map of jobids to urls 
	public static HashMap<String, String> callbackMap = new HashMap<String, String>();
	
	// The logger
    private static final Log LOG = LogFactory.getLog(ServerCallback.class);

	/**
	 * Register a callback url for a jobid.  The jobid value is appended to the
	 * url: http://apache.org becomes http://apache.org?jobid=...
	 * 
	 * @param jobid
	 * @param url
	 */
	public static void registerCallback(String jobid, String url) {
		if (url.indexOf("?") > -1) {
			url = url + "&jobid=" + jobid;
		} else {
			url = url + "?jobid=" + jobid;
		}
		callbackMap.put(jobid, url);
	}
	
	/**
	 * Call the callback url with the jobid to let them know it's finished.
	 * 
	 * @param jobid
	 * @throws CallbackFailedException
	 */
	public static void doCallback(String jobid) throws CallbackFailedException {
		HttpURLConnection connection = null;
		BufferedReader in = null;
		try {
			LOG.info("Calling " + callbackMap.get(jobid));
			URL url = new URL(callbackMap.get(jobid));
			connection = (HttpURLConnection) url.openConnection();			
		    in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		    String line;
		    while ((line = in.readLine()) != null) {
		    	//LOG.info(line + "\n");
		    }
		    in.close();
		} catch (Exception e) {
			throw new CallbackFailedException("Unable to connect to " + callbackMap.get(jobid));
		} finally {
		    try {
		    	in.close();
		    } catch (Exception e) {
		    	// Can't do anything with it.
		    }
		    try {
		    	connection.disconnect();
		    } catch (Exception e) {
		    	// Can't do anything with it.
		    }
		}
	}

}
