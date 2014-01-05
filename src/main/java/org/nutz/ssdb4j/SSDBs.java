package org.nutz.ssdb4j;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.nutz.ssdb4j.impl.SimpleClient;
import org.nutz.ssdb4j.pool.PoolSSDBStream;
import org.nutz.ssdb4j.pool.SocketSSDBStreamPool;
import org.nutz.ssdb4j.replication.ReplicationSSDMStream;
import org.nutz.ssdb4j.spi.SSDB;

public class SSDBs {
	

	public static String DEFAULT_HOST = "127.0.0.1";
	public static int DEFAULT_PORT = 8888;
	public static int DEFAULT_TIMEOUT = 2000;

	/**
	 * 使用默认配置生成一个单连接的客户端
	 */
	public static final SSDB simple() {
		return simple(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT);
	}
	
	/**
	 * 指定配置生成一个单连接的客户端
	 * @param host 主机名
	 * @param port 端口
	 * @param timeout 超时设置
	 */
	public static final SSDB simple(String host, int port, int timeout) {
		return new SimpleClient(host, port, timeout);
	}
	
	/**
	 * 指定配置生成一个使用连接池的客户端
	 * @param host 主机名
	 * @param port 端口
	 * @param timeout 超时设置
	 * @param config 连接池配置信息,如果为空,则使用默认值
	 */
	public static final SSDB pool(String host, int port, int timeout, Config config) {
		if (config == null)
			config = new Config();
		return new SimpleClient(new PoolSSDBStream(new SocketSSDBStreamPool(host, port, timeout, config)));
	}
	
	
	/**
	 * 按指定配置生成master/slave且使用连接池的客户端
	 * @param masterHost 主服务器的主机名
	 * @param masterPort 主服务器的端口
	 * @param slaveHost  从服务器的主机名
	 * @param slavePort  从服务器的端口
	 * @param timeout    超时设置
	 * @param config     连接池配置信息,如果为空,则使用默认值
	 */
	public static final SSDB replication(String masterHost, int masterPort, String slaveHost, int slavePort, int timeout, Config config) {
		if (config == null)
			config = new Config();
		PoolSSDBStream master = new PoolSSDBStream(new SocketSSDBStreamPool(masterHost, masterPort, timeout, config));
		PoolSSDBStream slave = new PoolSSDBStream(new SocketSSDBStreamPool(slaveHost, slavePort, timeout, config));
		return new SimpleClient(new ReplicationSSDMStream(master, slave));
	}
	
	public static String version() {
		return "7.5.2"; // 20130104 by wendal
	}
}
