package com.example.demo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 说明。。这个锁只竞争第一次。后面的全部不再竞争，同步执行，缓存的情况，适合此锁
 * @author jason
 * @createTime 2017年12月11日下午3:26:07
 */
public class DefaultDistributedLock implements DistributedLock, Watcher{

	private ZooKeeper zkClient;  
	  
    // 分布式锁持久化节点名称  
    private static String LOCK_PERSIST= "/DIS_LOCK";  
  
    // 临时节点前缀  
    private static String LOCK_ELEPHANT_PREFIX = LOCK_PERSIST+"/dis_";  
  
    // zk连接的ip  
    private static String ips = "10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181";  
  
    // session过期时间  
    private static int sessionTimeout = 300000;  
  
    // 主线程等待连接建立好后才启动  
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);  
  
    // 当前线程创建临时节点后返回的路径  
    private String selfPath;  
    // 等锁路径  
    private String waitPath;  
  
    private String lockName;  
  
    private CountDownLatch latch;  
    
    public DefaultDistributedLock(){
    	init();
    }
    
    public void init(){
    	new DefaultDistributedLock(ips,Thread.currentThread().getId()+"");
    }
  
    public DefaultDistributedLock(String ips, String lockName) {  
        this(ips,sessionTimeout, lockName);  
    }  
  
    public DefaultDistributedLock(String ips, int sessionTimeout, String lockName) {  
        this.ips = ips;  
        this.sessionTimeout = sessionTimeout;  
        this.lockName = lockName;  
        createRootNode(LOCK_PERSIST,"根节点");  
    }  
  
  
  
    public boolean dLock() {  
        try {  
            selfPath = zkClient.create(LOCK_ELEPHANT_PREFIX, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,  
                    CreateMode.EPHEMERAL_SEQUENTIAL);  
            System.out.println(lockName+" 创建临时节点路径"+selfPath);  
            return checkMinPath(selfPath);  
        } catch (Exception e) {  
            e.printStackTrace();  
            return false;  
        }  
    }  
  
    
    public boolean dLock(long time){  
        try {  
            if(dLock()){  
                return true;  
            }  
            return waitForLock(time);  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        return false;  
    }  
  
    public void unDLock()  {  
        System.out.println(lockName+"删除本节点：" + selfPath);  
        try {  
            zkClient.delete(selfPath, -1);  
            selfPath = null;  
            releaseConnection();  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        } catch (KeeperException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void releaseConnection() {  
        if (this.zkClient != null) {  
            try {  
                this.zkClient.close();  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
        }  
        System.out.println(lockName+"释放连接");  
    }  
  
    // 判断比自己小的一个节点是否存在，如果不存在，直接返回，无需等待  
    private boolean waitForLock(long t) throws KeeperException, InterruptedException {  
        Stat  stat = zkClient.exists(waitPath, true);  
        if(stat != null){  
            this.latch = new CountDownLatch(1);  
            System.out.println("###################"+lockName);
            // 如果超过等待时间会抛出异常  
            latch.await(t, TimeUnit.MILLISECONDS);  
            this.latch = null;  
        }  
        return true;  
    }  
  
  
    // 校验本线程创建的节点是否是所有节点中的最小节点  
    private boolean checkMinPath(String selfPath) throws KeeperException, InterruptedException {  
        List<String> subNodes = zkClient.getChildren(LOCK_PERSIST,false);  
        Collections.sort(subNodes);  
        String str = selfPath.substring(LOCK_PERSIST.length()+1);  
        int index = subNodes.indexOf(str);  
  
        switch (index){  
            case -1:{  
                System.out.println(lockName+"--本节点已不在了..." + selfPath);  
                return false;  
            }  
            case 0:{  
                System.out.println(lockName+"--本节点是最小节点..." + selfPath);  
                return true;  
            }  
            default:{  
                waitPath = LOCK_PERSIST+"/"+subNodes.get(index-1);  
                System.out.println(lockName+"--获取子节点中，排在我前面的"+ waitPath);  
                // 对前一个节点注册监听事件  
                try {  
                    zkClient.getData(waitPath,true,new Stat());  
                    return false;  
                } catch (Exception e) {  
                    // 跑出异常的时候，判断节点是否存在  
                   if(zkClient.exists(waitPath, false) == null){  
                       System.out.println(lockName+ "--子节点中，排在我前面的" + waitPath + "已失踪,重新检查");  
                       return checkMinPath(selfPath);  
                   }else{  
                       throw new RuntimeException(waitPath+"node disappered");  
                   }  
                }  
            }  
        }  
    }  
  
  
  
  
    // 创建根节点,根节点不需要进行watch  
    private boolean createRootNode(String path, String data) {  
        try {  
            createConnection();  
            if(zkClient.exists(path, false) == null){  
                String retPath = zkClient.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);  
                System.out.println("创建根节点:path" + retPath + "content" + data);  
            }  
            return true;  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        return false;  
    }  
  
  
    private void createConnection() throws IOException, InterruptedException {  
        if(zkClient == null){  
            zkClient = new ZooKeeper(ips, sessionTimeout, this);  
            connectedSemaphore.await();  
        }  
    }  
  
  
    public void process(WatchedEvent watchedEvent) {  
        if(watchedEvent == null){  
            return;  
        }  
  
        Event.EventType eventType = watchedEvent.getType();  
        Event.KeeperState state = watchedEvent.getState();  
  
        if(Event.KeeperState.SyncConnected == state){  
            if(Event.EventType.None == eventType){  
                System.out.println("正在启动连接服务器");  
                connectedSemaphore.countDown();  
            }/*else if (eventType == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {  
            	 System.out.println(lockName+ "--子节点中，排在我前面的" + waitPath + "已失踪,可以出山了！");  
            }*/  else if (Event.KeeperState.Disconnected == state) {  
                System.out.println("与ZK服务器断开连接");  
            } else if (Event.KeeperState.Expired == state) {  
                System.out.println("会话失效");  
            }  
        }  
    }
    
    private static final int THREAD_NUM = 10;  
    private static final CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);  
  
    public static void main(String[] args) {  
        final String ips = "10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181";  
        for(int i=0; i< THREAD_NUM;i++){  
            final int threadId = i + 1;  
            new Thread(){  
                @Override  
                public void run() {  
                    try {  
                        DistributedLock dc = new DefaultDistributedLock(ips, threadId + "");  
                        if (dc.dLock(2000)) {  
                            System.out.println(threadId+"获取到锁，并执行了任务");  
                            Thread.sleep(1000);
                            System.out.println("1111111111111111111");
                        }  
                        dc.unDLock();  
                        threadSemaphore.countDown();  
                    } catch (Exception e) {  
                        System.out.println("第" + threadId + "个线程抛出的异常：");  
                        e.printStackTrace();  
                    }  
                }  
            }.start();  
  
        }  
  
        try {  
            threadSemaphore.await();  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        }  
    } 

}
