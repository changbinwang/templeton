/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hcatalog.templeton;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jettison.json.JSONObject;

/**
 * This does periodic cleanup 
 */
public class ZookeeperCleanup extends TimerTask implements Watcher {
    protected AppConfig appConf;
    protected ExecService execService;
    protected static ZooKeeper zk = null;
    
    // The interval to wake up and check the queue
    protected static long interval = 3600000; // One hour
    
    // The max age of a task allowed
    protected static long maxage = 1000L * 60L * 60L * 24L * 7L; // One week
    
    protected static String root = "/templeton/cleanup";
    
    // The task list is a PERSISTENT_SEQUENTIAL list of tasks, each containing a json object with
    // name of the taskqueue object it's associated with and the create_date
    protected static String tasklist = "/templeton/tasklist";
    
    // The actual taskqueue, with arbitrarily large json objects, named with the jobid
    protected static String taskqueue = "/templeton/taskqueue";	
    
    // Mode to create files with
    protected static CreateMode mode = CreateMode.PERSISTENT;
    protected static CreateMode smode = CreateMode.PERSISTENT_SEQUENTIAL;
    
    // The logger
    private static final Log LOG = LogFactory.getLog(ZookeeperCleanup.class);
    
    public ZookeeperCleanup(AppConfig appConf) {
        this.appConf = appConf;
        long time = new Date().getTime();
        long ref = time;
        time = (time / interval) * interval; // Round to the nearest hour
        time += (interval/2);  // Add a half hour to avoid millisecond rounding problems
        if (time < ref) {
        	time += interval; // Make sure start date is in the future.
        }
        LOG.info("Process will start at : " + new Date(time).toString());
        new Timer().scheduleAtFixedRate(this, new Date(time), interval);
    }
    
    public void run() {
    	LOG.info("ZookeeperCleanup executing at " + new Date());
    	try {
	    	zk = new ZooKeeper(appConf.zkHosts(), appConf.zkSessionTimeout(), this);
    		//zk = new ZooKeeper("127.0.0.1:2181", 30000, this);
	    	Stat s = zk.exists(root, false);
		    if (s == null) {
		        zk.create("/templeton", new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
		        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
		    } 
		    if (zk.exists(tasklist, false) == null) {
		    	zk.create(tasklist, new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
		    }
		    if (zk.exists(taskqueue, false) == null) {
		    	zk.create(taskqueue, new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
		    }
		    
    	} catch (Exception e) {
    		LOG.error("ZookeeperCleanup failed to initialize");
    		e.printStackTrace();
    		return;
    	}
    	try {
    		String name = Long.toString(new Date().getTime() / interval);
    		LOG.info("Creating barrier " + name);
		    try {
		    	// Create the new node as a barrier to other processes.  If it tries to create the
		    	// file and it already exists, it skips to the NodeExistsException.  This is
		    	// why we leave each one there until the next round, so that late processes are
		    	// still blocked.
		        zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
		    	LOG.info("First!");
		        
		        // Delete old nodes
		        for (String child : zk.getChildren(root, false)) {
		        	if (!child.equals(name)) {
		        		zk.delete(root + "/" + child, -1);
		        	}
		        }
		        
		        // Children are named sequentially, so get them all and sort by sequence
		        List<String> children = zk.getChildren(tasklist, false);
		        Collections.sort(children, new Comparator<String>() {

					@Override
					public int compare(String o1, String o2) {
						try {
							int i1 = Integer.parseInt(o1.substring("joblist".length()));
							int i2 = Integer.parseInt(o2.substring("joblist".length()));
							return i1 - i2;
						} catch (Exception e) {
							return 0;
						}
					}
		        });
		        
		        // Delete tasks up to the current date
		        for (String child : children) {
	        		String tmp = "";
	        		// Put this in a separate try/catch so one failure won't halt the whole
	        		// process.
	        		try {
	        			Stat stat = null;
	        			tmp = new String(zk.getData(tasklist + "/" + child, false, stat));
	        			JSONObject jsonobject = new JSONObject(tmp);
	        			String jobid = jsonobject.getString("jobid");
	        			long created = jsonobject.getLong("datecreated");
	        			long now = new Date().getTime();
	        			if (now - created > maxage) {
	        				LOG.info("Deleting " + jobid);
	        				// Then delete the task first
	        				zk.delete(taskqueue + "/" + jobid, -1);
	        				
	        				LOG.info("Deleting task " + child);
	        				// Then the pointer to the task in the ordered queue
	        				zk.delete(tasklist + "/" + child, -1);
	        			} else {
	        				// They're sequential, so the first one that's too young means we're done.
	        				break;
	        			}
	        		} catch (Exception exc) {
	        			LOG.info("Unable to parse " + tmp);
	        			//zk.delete(tasklist + "/" + child, -1);
	        			// If it's unparseable, something got corrupted.  If we erase it, we
	        			// won't be able to find the original task.  Not sure what's best
	        			// to do here.
	        		}
		        }
		    } catch (KeeperException.NodeExistsException nee) {
		    	LOG.info("Blocked!");
		    }
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
    @Override
	public void process(WatchedEvent event) {
    	// In case we want to watch it later.
	}
    
    public static void main(String[] args) {
    	new ZookeeperCleanup(AppConfig.getInstance());
    }
}
