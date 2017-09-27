package com.example.demo.redissonlock;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.core.RAtomicLong;

public class RedissonManager {
	private static final String RAtomicName = "genId_";

    private static Config config = new Config();
    private static Redisson redisson = null;

   public static Redisson getRedisson(String ip,String port){  
        Config config=new Config();  
        config.useSingleServer().setAddress(ip+":"+port);  
        Redisson redisson=(Redisson) Redisson.create(config);  
        System.out.println("成功连接Redis Server"+"\t"+"连接"+ip+":"+port+"服务器");  
        return redisson;  
    }  
    
   public static void init(String key,String value){
        try {
/*            config.useClusterServers() //这是用的集群server
                    .setScanInterval(2000) //设置集群状态扫描时间
                    .setMasterConnectionPoolSize(10000) //设置连接数
                    .setSlaveConnectionPoolSize(10000)
                    .addNodeAddress("127.0.0.1:6379");*/
        	if(key==null || "".equals(key)){
        		key=RAtomicName;
        	}
        	
            redisson = getRedisson("10.17.1.204","6379");
            //清空自增的ID数字
            RAtomicLong atomicLong = redisson.getAtomicLong(key);
            long pValue=1;
            if(value!=null && !"".equals(value)){
            	pValue = Long.parseLong(value);
            }
            atomicLong.set(pValue);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Redisson getRedisson(){
        return redisson;
    }

    /** 获取redis中的原子ID */
    public static Long nextID(){
        RAtomicLong atomicLong = getRedisson().getAtomicLong(RAtomicName);
       //原子性的获取下一个ID，递增1 
       atomicLong.incrementAndGet();
        return atomicLong.get();
    }
}
