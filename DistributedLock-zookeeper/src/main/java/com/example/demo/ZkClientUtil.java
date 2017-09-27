package com.example.demo;

import java.util.Arrays;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClientUtil {
	private static final Logger logger = LoggerFactory.getLogger(ZkClientUtil.class);
	// 此demo使用的集群，所以有多个ip和端口
    /*private static String CONNECT_SERVER = "10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181";
    private static int SESSION_TIMEOUT = 3000;
    private static int CONNECTION_TIMEOUT = 3000;*/
	 
    public ZkClient zkClient ;
    
    public static void main(String[] args) throws InterruptedException {
    	ZkClientUtil zk = new ZkClientUtil("10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181",3000,3000);
    	//ZkClientUtil.create("/testZkLock/sub",ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
    	zk.create("/testZkLock/sub","",CreateMode.PERSISTENT_SEQUENTIAL);
    	//zk.create("/parndaLock","",CreateMode.PERSISTENT);
    	zk.subscribe("/testZkLock/sub000000003", "1");
    	//zk.subscribe("/testZkLock", "3");
    	//Thread.sleep(5000);
    	//zk.subscribe("/testZkLock/sub0000000004", "2");
    	//zk.update("/testZkLock/sub0000000018", "11111");
    	zk.delete("/testZkLock/sub0000000018");
    	for(String str:zk.getChildren("/testZkLock")){
    		System.out.println(str);
    	}
    	//ZkClientUtil.delete("/testZkLock");
    }
    
    public ZkClientUtil(String connect_server,int sessionTimeout,int connectionTimeout){
    	zkClient = new ZkClient(connect_server, sessionTimeout,connectionTimeout);
    }
    
    public void releaseConnection(){
    	if(null != zkClient){
    		zkClient.close();
    	}
    }
    /**
     * 新增节点
     * @param zkClient
     * @param path
     * @param data
     * @param createMode 节点类型
     * @return 创建的节点
     */
    public String create(String path,String data,CreateMode createMode){
    	// 如果不存在节点，就新建一个节点
    	String value = "";
    	if(!zkClient.exists(path)){
    		value = zkClient.create(path, data, createMode);
    		logger.info("创建节点成功：" + value);
    	}
    	return value;
    }
    
    /**
     * 新增节点
     * @param zkClient
     * @param path
     * @param data
     * @param createMode 节点类型
     * @return 创建的节点
     */
    public String create(String path,List<ACL> acl,CreateMode createMode){
    	// 如果不存在节点，就新建一个节点
    	String value = "";
    	if(!zkClient.exists(path)){
    		value = zkClient.create(path, null,acl,createMode);
    		logger.info("创建节点成功：" + value);
    	}
    	return value;
    }
    
    /**
     * @Title: delete 
     * @Description: TODO(删除指定节点) 
     * @param @param zkClient    设定文件 
     * @return void    返回类型 
     * @throws
     */
    public  boolean delete(String path){
    	try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        // 存在节点才进行删除
        if(zkClient.exists(path)){
            boolean flag = zkClient.delete(path);
            logger.info("删除节点" + path + (flag == true ? "成功！" : "失败！"));
            return flag;
        }else{
        	logger.info("删除节点" + path + "不存在");
        	return false;
        }
        
    }
    
    /**
     * @Title: 删除节点下所有的目录
     * @Description: TODO(递归删除) 
     * @param @param zkClient    设定文件 
     * @return void    返回类型 
     * @throws
     */
    public boolean deletePath(String path){
        // 存在节点才进行删除
        if(zkClient.exists(path)){
            // 递归删除的时候 只传入 父节点就可以，如果传入 全部的节点，虽然返回的是true，但是依然是没有删除的，
            // 因为zkClient将异常封装好了，进入catch的时候，会返回true，这是一个坑
            boolean flag = zkClient.deleteRecursive(path);  
            logger.info("删除节点" + path + (flag == true ? "成功！" : "失败！"));
            return flag;
        }else{
        	logger.info("删除节点" + path + "不存在");
        	return false;
        }
    }
    
    /**
     * @Title: update 
     * @Description: TODO(修改节点的值) 
     * @param @param zkClient    设定文件 
     * @return void    返回类型 
     * @throws
     */
    public String update(String path,String value){
        if(zkClient.exists(path)){
            zkClient.writeData(path, value);
            // 查询一下，看是否修改成功
            String updateValue = zkClient.readData(path);
            logger.info("修改节点" + path + "成功,值为:" + updateValue);
            return updateValue;
        }else{
        	logger.info("修改节点" + path + "不存在");
        	return "";
        }
    }
    
    /**
     * 获取节点信息
     * @param path
     * @return
     */
    public Object get(String path){
    	if(zkClient.exists(path)){
            Object obj = zkClient.readData(path);
            logger.info("获取节点" + path + "成功,值为:" + obj.toString());
            return obj;
        }else{
        	logger.info("获取节点" + path + "不存在");
        	return "";
        }
    }
    
    public List<String> getChildren(String path){
    	if(zkClient.exists(path)){
            return zkClient.getChildren(path);
        }else{
        	logger.info("获取节点" + path + "不存在");
        	return null;
        }
    }
    
    
    /**
     * 查询节点是否存在
     * @param path
     * @return
     */
    public boolean exists(String path){
    	return zkClient.exists(path);
    }
    
    /**
      * @Title: subscribe 
     * @Description: TODO(事件订阅, 可用于配置管理) 
     * 先订阅，再 操作增删改。（可多个 客户端订阅）
     * @param @param path
     * @param types :1:订阅数据改变事件 2:订阅状态改变事件 3：订阅节点改变事件
     * @return void    返回类型 
     * @throws
     */
    public void subscribe(String path,String... types){
    	List<String> list = Arrays.asList(types);
    	if(list.contains("1")){
	        zkClient.subscribeDataChanges(path, new IZkDataListener() {
	            @Override
	            public void handleDataDeleted(String arg0) throws Exception {
	                System.out.println("触发了删除事件：" + arg0);
	            }
	            
	            @Override
	            public void handleDataChange(String arg0, Object arg1) throws Exception {
	                System.out.println("触发了改变事件：" + arg0 + "-->" + arg1);
	            }
	            
	        });
    	}
    	if(list.contains("2")){
	        zkClient.subscribeStateChanges(new IZkStateListener(){
	
				@Override
				public void handleNewSession() throws Exception {
					System.out.println("......");
				}
	
				@Override
				public void handleStateChanged(KeeperState paramKeeperState)
						throws Exception {
					System.out.println("###########"+paramKeeperState);
					System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$");
				}

				@Override
				public void handleSessionEstablishmentError(Throwable arg0)
						throws Exception {
					// TODO Auto-generated method stub
					
				}
	        	
	        });
    	}
        
    	if(list.contains("3")){
	        /** 
	         * 订阅节点的信息改变（创建节点，删除节点，添加子节点）
	         * path: 当前节点
	         */ 
	        zkClient.subscribeChildChanges(path, new IZkChildListener(){
	        	
	        	/** 
	             * handleChildChange： 用来处理服务器端发送过来的通知 
	             * parentPath：对应的父节点的路径 
	             * currentChilds：子节点的相对路径 
	             */
				@Override
				public void handleChildChange(String parentPath,
						List<String> currentChilds) throws Exception {
					System.out.println("###########"+parentPath+":"+currentChilds);
					
				}
	        });
    	}        
    }
}
