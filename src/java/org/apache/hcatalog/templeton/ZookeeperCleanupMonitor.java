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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jettison.json.JSONObject;

/**
 * This does periodic cleanup controlled by ZookeeperCleanupExecutor. 
 * It uses the leader election recipe to designate
 * which process is in charge of cleaning up, and if it loses, watches the
 * winner for disconnection to start a new election.
 */
public class ZookeeperCleanupMonitor implements Watcher {
    
    // The connection to Zookeeper from ZookeeperCleanupExecutor
    protected ZooKeeper zk = null;

    // Where the timestamp and election files go
    protected static String root = "/templeton/ELECTION";

    // The task list is a PERSISTENT_SEQUENTIAL list of tasks, each containing a
    // json object with
    // name of the taskqueue object it's associated with and the create_date
    protected static String tasklist = "/templeton/tasklist";

    // The actual taskqueue, with arbitrarily large json objects, named with the
    // jobid
    protected static String taskqueue = "/templeton/taskqueue";

    // The timestamp file
    protected static String timestamp = tasklist + "/timestamp";

    // The logger
    private static final Log LOG = LogFactory
            .getLog(ZookeeperCleanupMonitor.class);

    // The interval to wake up and check the queue
    protected static long interval = 3600000; // One hour

    // The max age of a task allowed
    protected static long maxage = 1000L * 60L * 60L * 24L * 7L; // One week

    // The node this particular process owns
    private String mynode = null;

    // The node owned by the process (may not be this one) that's currently
    // owning cleanup
    private String watchnode = null;

    // Signal the executor if the connection dies
    public boolean done = false;

    // The watcher to assign to the node
    private Watcher watcher;

    /*
     * Set variables and start the process
     */
    public ZookeeperCleanupMonitor(ZooKeeper zk, String mynode, Watcher watcher) {
        try {
            this.zk = zk;
            this.watcher = watcher;
            this.mynode = mynode;            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the current child list, see if the node matches our node.  If so,
     * we're the leader; start cleanup.
     */
    public void doWatch() {
        try {
            List<String> children = zk.getChildren(root, false);
            Collections.sort(children, new NodeComparator());
            watchnode = root + "/" + children.get(0);
           
            if (mynode.equals(watchnode)) {
                initialize();
                doCleanup();
            } else {
                zk.exists(watchnode, watcher);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This is the data monitor; called when the leader node changes.
     */
    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getState() == KeeperState.Expired) {
                LOG.error("The Zookeeper connection has expired.");
                ((ZookeeperCleanupExecutor) watcher).closing();
                return;
            }
                
            if (event.getState() == KeeperState.AuthFailed) {
                LOG.error("The Zookeeper authorization failed.");
                ((ZookeeperCleanupExecutor) watcher).closing();
                return;
            }
            
            if (event.getState() == KeeperState.Disconnected) {
                LOG.error("The Zookeeper connection has closed.");
                ((ZookeeperCleanupExecutor) watcher).closing();
                return;
            }
            if (watchnode != null && zk.exists(watchnode, false) == null) {
                doWatch();
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This is the actual cleanup -- read all task listings in chronological order,
     * deleting each one that's older than the interval.
     */
    public void doCleanup() {
        LOG.info("ZookeeperCleanup executing at " + new Date());
        try {
            checkTimestamp();
            
            LOG.info("Cleaning up " + new Date());

            // Children are named sequentially, so get them all and sort by
            // sequence
            List<String> children = zk.getChildren(tasklist, false);
            Collections.sort(children, new NodeComparator());

            // Delete tasks up to the current date
            for (String child : children) {
                String tmp = "";
                // Put this in a separate try/catch so one failure won't halt
                // the whole
                // process.
                try {
                    Stat stat = null;
                    tmp = new String(zk.getData(tasklist + "/" + child, false,
                            stat));
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
                        // They're sequential, so the first one that's too young
                        // means we're done.
                        break;
                    }
                } catch (Exception exc) {
                    LOG.info("Unable to parse " + tmp);
                    // zk.delete(tasklist + "/" + child, -1);
                    // If it's unparseable, something got corrupted. If we erase
                    // it, we
                    // won't be able to find the original task. Not sure what's
                    // best
                    // to do here.
                }
            }

            updateTimestamp();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Just make sure the correct directories exist.
     */
    public void initialize() {
        try {
            if (zk.exists(tasklist, false) == null) {
                zk.create(tasklist, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            if (zk.exists(taskqueue, false) == null) {
                zk.create(taskqueue, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

        } catch (Exception e) {
            LOG.error("ZookeeperCleanup failed to initialize");
            e.printStackTrace();
            return;
        }
    }

    /**
     * Check the old timestamp, if it exists.  If we're early, sleep until the
     * next check time.
     * @throws Exception
     */
    public void checkTimestamp() throws Exception {
        LOG.info("Checking timestamp " + new Date());
        if (zk.exists(timestamp, false) != null) {
            Date now = new Date();
            Date toExecute = new Date(Long.parseLong(new String((zk
                    .getData(timestamp, false, new Stat())))));
            if (toExecute.after(new Date())) {
                LOG.info("Sleeping "
                        + (toExecute.getTime() - now.getTime()));
                Thread.sleep(toExecute.getTime() - now.getTime());
                LOG.info("Woke up at " + new Date());
            }
        }
    }

    /**
     * Create the new timestamp.
     * 
     * @throws Exception
     */
    public void updateTimestamp() throws Exception {
        LOG.info("Creating timestamp " + new Date());
        if (zk.exists(timestamp, false) != null) {
            zk.delete(timestamp, -1);
        }
        zk.create(timestamp, Long.toString(new Date().getTime() + interval)
                .getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Thread.sleep(interval);
        doCleanup();
    }

    /**
     * A simple comparator that orders the list of nodes, because getChildren() doesn't
     * guarantee an order (why not?).  This assumes the file has been given the
     * form [name]_### 
     *
     */
    class NodeComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            try {
                int i1 = Integer
                        .parseInt(o1.substring(o1.lastIndexOf("_") + 1));
                int i2 = Integer
                        .parseInt(o2.substring(o2.lastIndexOf("_") + 1));
                return i1 - i2;
            } catch (Exception e) {
                return 0;
            }
        }
    }
}
