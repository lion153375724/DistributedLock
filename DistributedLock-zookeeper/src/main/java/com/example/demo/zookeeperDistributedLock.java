package com.example.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 主要是利用zookeeperr的EPHEMERAL_SEQUENTIAL节点特性,生成临时顺序节点,
 * 节点值最小的id获取锁,进行业务操作后,释放锁,
 * 其它进程订阅节点的状态变化情况，如果当前锁已释放或不存在，就重新竞争锁，最小的id节点获取锁。
 * 
 * @author Administrator
 *
 */
public class zookeeperDistributedLock implements Watcher{
	private static final Logger logger = LoggerFactory.getLogger(zookeeperDistributedLock.class);
	private static final String CONNECT_SERVER = "10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181";
	private static final String GROUP_PATH = "/zkDistributedLock";
	private static final String SUB_PATH = "/zkDistributedLock/sub";
	private static final int SESSION_TIMEOUT = 5000;
	private static final int THREAD_NUM = 10;
	
	//确保连接zk成功；  
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);  
    //确保所有线程运行结束；  
    private CountDownLatch threadSemaphore = new CountDownLatch(1);  
	
	private String selfPath = ""; //当前节点
	private String waitPath = ""; //当前节点的前一个节点
	private ZooKeeper zk;
	
	public zookeeperDistributedLock(){
		
	}
	public static void main(String[] args) {
		for(int i=0; i<THREAD_NUM; i++){
			Thread thread = new Thread(new Runnable(){
				@Override
				public void run() {
					zookeeperDistributedLock zkLock = new zookeeperDistributedLock();;
					try {
						zkLock.createConnection();
						//创建根节点
						synchronized (zkLock.threadSemaphore) {
							zkLock.cratePath(GROUP_PATH, "zkDistributedLock", true);
						}
						if(zkLock.lock()){
							//业务处理
							zkLock.processBusiness();
						}
					} catch (Exception e) {
						// TODO: handle exception
					}
				}
			});
			thread.start();
		}
		/*try {
			threadSemaphore.await();
			logger.info("所有线程运行结束！");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
	}
	
	public void processBusiness() throws InterruptedException, KeeperException{
		logger.info(Thread.currentThread().getName()+":抢锁成功,执行相关业务流程Start");
		Thread.sleep(1000);
		unlock();
		
	}
	
	public void createConnection() throws Exception{
		if(null == zk){
			zk = new ZooKeeper(CONNECT_SERVER,SESSION_TIMEOUT,this);
		}
		connectedSemaphore.await();
		
		
	}
	
	public void cratePath(String path,String data,boolean needWatch) throws KeeperException, InterruptedException{
		if(null == zk.exists(path, needWatch)){
			if(null != zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)){
				logger.info(Thread.currentThread().getName()+":创建根节点成功：" + path);
			}
		}
	}
	
	public boolean lock() throws KeeperException, InterruptedException{
		try {
			selfPath = zk.create(SUB_PATH, null,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			if(!StringUtils.isEmpty(selfPath)){
				logger.info(Thread.currentThread().getName()+":创建子节点：" + selfPath);
			}else{
				logger.info(Thread.currentThread().getName()+":创建子节点失败：" + selfPath);
			}
		} catch (Exception e) {
			logger.info(Thread.currentThread().getName()+":创建子节点失败：" + selfPath);
			e.printStackTrace();
		}
		if(isMinPath()){
			logger.info(Thread.currentThread().getName()+":抢锁成功,该干活了！节点：" + selfPath);
			return true;
		}
		return false;
	}
	
	public void unlock(){
		
		try {
			logger.info(Thread.currentThread().getName()+":抢锁成功,执行相关业务流程End");
			zk.delete(selfPath, -1);
			logger.info(Thread.currentThread().getName()+"删除节点成功：" + selfPath);
		} catch (Exception e) {
			logger.info(Thread.currentThread().getName()+"删除节点错误：" + selfPath);
			e.printStackTrace();
		}
		
		try {
			zk.close();
		} catch (InterruptedException e) {
			logger.info(Thread.currentThread().getName()+"释放连接错误！");
			e.printStackTrace();
		}
	}
	
	public boolean isMinPath() throws KeeperException, InterruptedException{
		List<String> childrenList = new ArrayList<String>();
		childrenList = zk.getChildren(GROUP_PATH, false);
		Collections.sort(childrenList);
		int index = childrenList.indexOf(selfPath.substring(GROUP_PATH.length()+1));
		switch(index){
			case -1:{
				logger.info(Thread.currentThread().getName()+":该节点已不存在！" + selfPath);
				return false;
			}
			case 0:{
				logger.info(Thread.currentThread().getName()+":我果然是老大！" + selfPath);
				return true;
			}
			default :{
				waitPath = GROUP_PATH + "/" + childrenList.get(index-1);
				logger.info(Thread.currentThread().getName()+":排在我前面的节点是 ：！" + waitPath);
				zk.getData(waitPath, true, new Stat()); 
				if(null == waitPath){
					return false;
				}else{
					threadSemaphore.await();
				}
				return false;
			}
		}
	}
	

	
	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == Event.KeeperState.SyncConnected){
			if(event.getType() == Event.EventType.None){
				logger.info(Thread.currentThread().getName()+":成功连接服务器！");
				connectedSemaphore.countDown();
			}else{
				if(event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)){
					threadSemaphore.countDown();
					logger.info(Thread.currentThread().getName()+":收到情报,排在我前面的大哥已经挂了 ：！" + waitPath);
					//重新竞争
					try {
						if(isMinPath()){
							processBusiness();
						}
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}else if(event.getState() == Event.KeeperState.Disconnected){
			logger.info(Thread.currentThread().getName()+":与服务器断开连接！");
		}else if(event.getState() == Event.KeeperState.AuthFailed){
			logger.info(Thread.currentThread().getName()+":授权失败！");
		}else if(event.getState() == Event.KeeperState.Expired){
			logger.info(Thread.currentThread().getName()+":会话失效！");
		}
	}
	
}
