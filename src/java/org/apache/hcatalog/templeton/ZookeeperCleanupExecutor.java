package org.apache.hcatalog.templeton;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZookeeperCleanupExecutor implements Watcher, Runnable {

    ZookeeperCleanupMonitor dataMonitor = null;
    ZooKeeper zk;

    // Where the timestamp and election files go
    protected static String root = "/templeton/ELECTION";

    /**
     * Connect to zookeeper and initialize the node.
     * 
     * @param appConf
     */
    public ZookeeperCleanupExecutor(AppConfig appConf) {
        try {
            zk = new ZooKeeper(appConf.zkHosts(), appConf.zkSessionTimeout(),
                    this);
            if (zk.exists("/templeton", false) == null) {
                zk.create("/templeton", new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            if (zk.exists(root, false) == null) {
                zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            String mynode = zk.create(root + "/" + "n_", new byte[0],
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            dataMonitor = new ZookeeperCleanupMonitor(zk, mynode, this);
            dataMonitor.doWatch(); // Start things off.
        } catch (Exception e) {
            System.out.println("Zookeeper connection failed to start");
            e.printStackTrace();
        }

    }

    /**
     * Just pass events through to the monitor (ZookeeperCleanupMonitor)
     */
    public void process(WatchedEvent event) {
        dataMonitor.process(event);
    }

    /**
     * Wait until we get the go-ahead to check if we're leader again.
     */
    public void run() {
        try {
            synchronized (this) {
                while (!dataMonitor.done) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
        }
    }

    /**
     * Shut everything down.
     * 
     */
    public void closing() {
        synchronized (this) {
            dataMonitor.done = true;
            notifyAll();
        }
    }

    /**
     * Start it from the command line.
     * 
     * @param args
     */
    public static void main(String[] args) {
        new ZookeeperCleanupExecutor(AppConfig.getInstance()).run();
    }

}
