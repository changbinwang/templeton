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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Date;

import org.apache.hcatalog.templeton.tool.JobState;
import org.apache.hcatalog.templeton.tool.JobTracker;
import org.apache.zookeeper.ZooKeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This does periodic cleanup
 */
public class ZookeeperCleanup  {
    protected AppConfig appConf;

    // The interval to wake up and check the queue            
    public static final String ZK_CLEANUP_INTERVAL = "templeton.zookeeper.cleanup.interval"; // 12 hours
    
    // The max age of a task allowed    
    public static final String ZK_CLEANUP_MAX_AGE = "templeton.zookeeper.cleanup.maxage"; // ~ 1 week
    
    protected static long interval = 1000L * 60L * 60L * 12L;
    protected static long maxage = 1000L * 60L * 60L * 24L * 7L;

    // The logger
    private static final Log LOG = LogFactory.getLog(ZookeeperCleanup.class);

    // Handle to cancel loop
    private boolean stop = false;

    /**
     * Create a cleanup object.  We use the appConfig to configure JobState.
     * @param appConf
     */
    public ZookeeperCleanup(AppConfig appConf) {
        this.appConf = appConf;
        interval = appConf.getLong(ZK_CLEANUP_INTERVAL, interval);
        maxage = appConf.getLong(ZK_CLEANUP_MAX_AGE, maxage);
    }

    /**
     * Run the cleanup loop.
     *
     * @throws IOException
     */
    public void doCleanup() throws IOException {   
        ZooKeeper zk = null;
        List<String> nodes = null;
        while (!stop) {
            try {
                // Put each check in a separate try/catch, so if that particular
                // cycle fails, it'll try again on the next cycle.
                try {
                    LOG.info("Getting children");
                    zk = JobState.zkOpen(AppConfig.getInstance());
    
                    nodes = getChildList(zk);
    
                    for (String node : nodes) {
                        LOG.info("Checking " + node);
                        boolean deleted = checkAndDelete(node, zk);
                        if (!deleted) {
                            break;
                        }
                    }
                    
                    zk.close();
                } catch (Exception e) {
                    LOG.error("Cleanup cycle failed: " + e.getMessage());
                } finally {
                    if (zk != null) {
                        try {
                            zk.close();
                        } catch (InterruptedException e) {
                            // We're trying to exit anyway, just ignore.
                        }
                    }
                }
                
                long sleepMillis = (long) (Math.random() * interval);
                LOG.info("Next execution: " + new Date(new Date().getTime()
                                                       + sleepMillis));
                Thread.sleep(sleepMillis);

            } catch (Exception e) {
                // If sleep fails, we should exit now before things get worse.
                throw new IOException("Cleanup failed: " + e.getMessage(), e);
            } 
        }

    }

    /**
     * Get the list of jobs from JobState
     *
     * @return
     * @throws IOException
     */
    public List<String> getChildList(ZooKeeper zk) {
        try {
            List<String> jobs = JobTracker.getTrackingJobs(appConf, zk);
            Collections.sort(jobs);
            return jobs;
        } catch (IOException e) {
            LOG.info("No jobs to check.");
        }
        return new ArrayList<String>();
    }

    /**
     * Check to see if a job is more than maxage old, and delete it if so.
     * @param state
     */
    public boolean checkAndDelete(String node, ZooKeeper zk) {
        try {
            JobTracker tracker = new JobTracker(node, zk, true);
            long now = new Date().getTime();
            long then = tracker.getDateCreated();
            if (now - then > maxage) {
                LOG.info("Deleting " + tracker.getJobID());
                JobState state = new JobState(tracker.getJobID(), zk);
                state.delete();
                tracker.delete();
                return true;
            } 
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.info("checkAndDelete failed for " + node);
            // We don't throw a new exception for this -- just keep going with the
            // next one.
            return true;
        }
    }

    // Handle to stop this process from the outside if needed.
    public void stop() {
        stop = true;
    }

    public static void main(String[] args) {
        ZookeeperCleanup zc = new ZookeeperCleanup(AppConfig.getInstance());
        try {
            zc.doCleanup();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

}
