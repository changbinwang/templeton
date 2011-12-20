package org.apache.hcatalog.templeton.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jettison.json.JSONObject;

public class JobTracker {
    // The root of the tracking nodes
    public static final String JOB_TRACKINGROOT = JobState.JOB_ROOT + "/created";
    
    // The key for the created date
    public static final String DATECREATED = "datecreated";
    
    // The key for jobid
    public static final String JOBID = "jobid";

    // The zookeeper connection to use
    private ZooKeeper zk;
    
    // The id of the tracking node -- must be a SEQUENTIAL node
    private String trackingnode;
    
    // The id of the job this tracking node represents
    private String jobid;
    
    /**
     * Constructor for a new node -- takes the jobid of an existing job
     * 
     * @param jobid
     * @param zk
     */
    public JobTracker(String node, ZooKeeper zk, boolean nodeIsTracker) {
        this.zk = zk;
        if (nodeIsTracker) {
            trackingnode = node;
        } else {
            jobid = node;
        }
    }
    
    /**
     * Create the parent znode for this job state.
     */
    public void create(long create_date)
        throws IOException
    {
        String[] paths = {JobState.JOB_ROOT, JOB_TRACKINGROOT};
        for (String znode : paths) {
            try {
                zk.create(znode, new byte[0],
                          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
            } catch (Exception e) {
                throw new IOException("Unable to create parent nodes");
            }
        }
        try {
            JSONObject jo = new JSONObject();
            jo.put(DATECREATED, create_date);
            jo.put(JOBID, jobid);
            trackingnode = zk.create(makeTrackingZnode(), jo.toString().getBytes(), 
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (Exception e) {
            throw new IOException("Unable to create " + makeTrackingZnode());
        }
    }

    public void delete()
        throws IOException
    {
        try {
            zk.delete(makeTrackingJobZnode(trackingnode), -1);
        } catch (Exception e) {
            // Might have been deleted already
            System.out.println("Couldn't delete " + makeTrackingJobZnode(trackingnode));
        }
    }
    
    /**
     * Get the create date for a tracking node
     * @param key
     * @return
     * @throws IOException
     */
    public long getDateCreated() throws IOException {
        try {
            return Long.parseLong(getField(DATECREATED));
        } catch (Exception e) {
            throw new IOException("Couldn't parse value for datecreated as a long.");
        }
    }
    
    /**
     * Get the jobid for this tracking node
     * @return
     * @throws IOException
     */
    public String getJobID() throws IOException {
        return getField(JOBID);
    }
    
    /**
     * Get a value from this tracking node
     * 
     * @param key
     * @return
     * @throws IOException
     */
    public String getField(String key) throws IOException {
        try {
            JSONObject jo = new JSONObject( new String(
                    zk.getData(makeTrackingJobZnode(trackingnode), false, new Stat())));
            return jo.get(key).toString();
        } catch (Exception e) {
            throw new IOException("Couldn't read node " + trackingnode);
        }
    }
    
    /**
     * Make a ZK path to a new tracking node
     */
    public String makeTrackingZnode() {
        return JOB_TRACKINGROOT + "/jobcreated_";
    }
    
    /**
     * Make a ZK path to an existing tracking node
     */
    public String makeTrackingJobZnode(String nodename) {
        return JOB_TRACKINGROOT + "/" + nodename;
    }
    
    /*
     * Get the list of tracking jobs.  These can be used to determine which jobs have
     * expired.
     */
    public static List<String> getTrackingJobs(Configuration conf, ZooKeeper zk) throws IOException {
        ArrayList<String> jobs = new ArrayList<String>();
        try {
            for (String myid : zk.getChildren(JOB_TRACKINGROOT, false)) {
                jobs.add(myid);
            }
        } catch (Exception e) {
            throw new IOException("Can't get tracking children", e);
        }
        return jobs;
    }
}
