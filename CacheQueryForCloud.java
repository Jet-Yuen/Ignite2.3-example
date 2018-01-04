package com.bsfit.data.service.job;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.bsfit.data.service.model.JetAddress;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;
import javax.cache.Cache;
import javax.cache.Cache.Entry;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

/**
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-core</artifactId>
            <version>2.3.0</version>
        </dependency>
 * @author Jet
 *
 */
@Component
public class CacheQueryForCloud {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    private Ignite ignite;
    
    @Value("${app.ignate.cache.model:1}")
    private int model;
    
    @Value("${app.common.cacheFilePath}")
    private String storePath;
    
    @Value("${app.ignate.finder.ipport:127.0.0.1:17500}")
    private String finder;
    
    @Value("${app.ignate.local.ip:127.0.0.1}")
    private String localIp;
    
    @Value("${app.ignate.local.port:17500}")
    private int localPort;
    
    @Value("${app.ignate.communication.port:17000}")
    private int communicationPort;
    
    @Value("${app.ignate.sql.port:17100}")
    private int sqlPort;
    
    @Value("${app.ignate.memory.initsize:#{100 * 1024 * 1024}}")//100MB
    private Long initSize;
    
    @Value("${app.ignate.memory.maxsize:#{1024 * 1024 * 1024}}")//1GB
    private Long maxSize;
    
    @Value("${app.ignate.memory.expiry:10}")//10mins
    private Long expiry;
    
    @Value("${app.ignate.persistence:true}")
    private boolean persistence;
    
    private final String ADDRESS_CACHE = "Address";
    
    @PostConstruct
    public void doJob() {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    init();
                }
            }, 1_000L);
    } 
    
    public void init() {
        //设置集群组播查找，设置本机ip/127.0.0.1。 
        //finder设置其他节点ip:port
        //TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        
        //设置静态IP查找，设置本机ip/127.0.0.1。 
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();//静态ip查找
        ipFinder.setAddresses(Arrays.asList(localIp, finder));
        
        //设置集群的本地侦听端口。建议固定一个端口
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setLocalPort(localPort);
        spi.setLocalPortRange(0);
        spi.setIpFinder(ipFinder);
        
        //设置集群的数据通讯的本地侦听端口
        TcpCommunicationSpi ipCom = new TcpCommunicationSpi();
        ipCom.setLocalPort(communicationPort);
        ipCom.setMessageQueueLimit(32);//避免队列过大导致OOME
        
        //固化内存，能使用的堆外内存大小，用于存储缓存数据。与-XX:MaxDirectMemorySize配置一致
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setInitialSize(initSize);
        drCfg.setMaxSize(maxSize);
        if (persistence) {
            //数据本地持久化，如果开启,则重启不会丢失缓存数据，可避免应该缓存的数据因内存不足而丢弃的问题。
            //如果开启,则当日志发现“Page evictions starts”，说明内存不够，数据大量写入磁盘，性能低，建议设置更大的内存
            drCfg.setPersistenceEnabled(true);
            dsCfg.setWalMode(WALMode.LOG_ONLY);//调优建议
            dsCfg.setWriteThrottlingEnabled(true);//调优建议,关于Checkpoint
        } else {
            //当内存使用达到阈值(默认90%)时，最老的数据会被删除
            drCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        }
        
        dsCfg.setDefaultDataRegionConfiguration(drCfg);//默认内存策略
        
        //缓存配置
        CacheConfiguration<Long, JetAddress> cacheCfg = new CacheConfiguration<>(ADDRESS_CACHE);
        cacheCfg.setIndexedTypes(Long.class, JetAddress.class);
        cacheCfg.setSqlFunctionClasses(IgniteFunction.class);
        cacheCfg.setSqlSchema("PUBLIC");
        
        if (model == 0) {
            //缓存“分区”模式，各节点保持一部份数据。
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);
            //设置缓存备份，当一个节点关闭时，另一个节点备份的数据就会使用上。
            cacheCfg.setBackups(1);
        } else {
            //缓存“复制”模式，各节点保持全数据，无须备份，查询性能更好
            cacheCfg.setCacheMode(CacheMode.REPLICATED);
        }
        
        
        //设置缓存数据过期时间，默认10分钟
        cacheCfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(MINUTES, expiry)));
        
        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setCommunicationSpi(ipCom);
        igniteCfg.setDiscoverySpi(spi);
        igniteCfg.setCacheConfiguration(cacheCfg);
        igniteCfg.setWorkDirectory(storePath);//工作目录，数据存储路径
        igniteCfg.setDataStorageConfiguration(dsCfg);
        
        //Logger logger = LoggerFactory.getLogger("org.apache.ignite");
        igniteCfg.setGridLogger(new Slf4jLogger(logger));
        
        ignite = Ignition.start(igniteCfg);
        ignite.active(true);//必须阻塞等待激活
        logger.info("Cache example started.");
     
        IgniteCache<Long, JetAddress> addrCache = ignite.getOrCreateCache(cacheCfg);
        
        IgniteAtomicSequence atomicSequence = ignite.atomicSequence("JetAtomicSequence", 0, true);
        logger.info("{}", atomicSequence.get());
        long id = atomicSequence.incrementAndGet();
        
        //Insert
        SqlFieldsQuery qry = new SqlFieldsQuery(
                "insert into JetAddress (_key, id, province) values (?, ?, ?)");
        addrCache.query(qry.setArgs(id, id, "浙江省"));
        
        // Put
        JetAddress ja2 = new JetAddress();
        ja2.setId(atomicSequence.incrementAndGet());
        ja2.setProvince("浙江省2");
        addrCache.put(atomicSequence.get(), ja2);
        
        //Get
        JetAddress ja = addrCache.get(id);
        logger.info("Get province:{}", ja.getProvince());
        
        //SqlFieldsQuery 
        String sql = "select province from JetAddress where province like '浙%' limit 1000";
        SqlFieldsQuery query = new SqlFieldsQuery(sql);
        
        try (QueryCursor<List<?>> cursor = addrCache.query(query))  {
            for (List<?> entry : cursor) {
                logger.info("SqlFieldsQuery province:{}", entry.toString());
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        
        //SqlQuery 
        SqlQuery<Long, JetAddress> query2 = new SqlQuery<Long, JetAddress>(JetAddress.class, " matchFun(province, ?)=1");
        try (QueryCursor<Cache.Entry<Long, JetAddress>> cursor = addrCache.query(query2.setArgs("浙")))  {
            for (Entry<Long, JetAddress> entry : cursor) {
                JetAddress jAddress = entry.getValue();
                logger.info("SqlQuery province:{}", jAddress.getProvince());
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        
        logger.info("Cache example finished.");
    }
    
}
