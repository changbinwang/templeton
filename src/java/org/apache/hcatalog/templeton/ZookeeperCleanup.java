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
import java.util.Date;

import org.apache.hcatalog.templeton.tool.JobState;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This does periodic cleanup 
 */
public class ZookeeperCleanup  {
    protected AppConfig appConf;
    
    // The interval to wake up and check the queue
    protected static long interval = 1000L * 60L * 60L * 12L; // One hour
    
    // The max age of a task allowed
    protected static long maxage = 1000L * 60L * 60L * 24L * 7L; // One week
    
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
    }
    
    /**
     * Run the cleanup loop.
     * 
     * @throws IOException
     */
    public void doCleanup() throws IOException {
    
        while (!stop) {
            try {
                LOG.info("Getting children");
                ArrayList<JobState> states = getChildList();
                
                for (JobState state : states) {
                    LOG.info("Checking " + state.makeZnode());
                    checkAndDelete(state);
                }
                
                long sleepMillis = (long) (Math.random() * interval);
                LOG.info("Next execution: " + new Date(new Date().getTime() + sleepMillis));
                Thread.sleep(sleepMillis);
                
            } catch (Exception e) {
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
    public ArrayList<JobState> getChildList() {
        JobState js;
        try {
            js = new JobState("", appConf);
            return js.getJobs(appConf);
        } catch (IOException e) {
            LOG.info("No jobs to check.");
        }
        return new ArrayList<JobState>();
    }
    
    /**
     * Check to see if a job is more than maxage old, and delete it if so.
     * @param state
     */
    public void checkAndDelete(JobState state) {
        try {
            long now = new Date().getTime();
            long then = state.getCreated();
            if (now - then > maxage) {
                LOG.info("Deleting " + state.makeZnode());
                state.delete();
            }
        } catch (Exception e) {
            LOG.info("checkAndDelete failed for " + state.makeZnode());
            // We don't throw a new exception for this -- just keep going with the
            // next one.
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
