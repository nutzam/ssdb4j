package org.nutz.ssdb4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.nutz.ssdb4j.impl.RawClient;
import org.nutz.ssdb4j.impl.SimpleClient;
import org.nutz.ssdb4j.pool.PoolSSDBStream;
import org.nutz.ssdb4j.pool.SocketSSDBStreamPool;
import org.nutz.ssdb4j.replication.ReplicationSSDMStream;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Respose;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;

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
	
	public static byte[] readBlock(InputStream in) throws IOException {
		int len = 0;
		int d = in.read();
		if (d == '\n')
			return null;
		else if (d >= '0' && d <= '9')
			len = len * 10 + (d - '0');
		else
			throw new SSDBException("protocol error. unexpect byte=" + d);
		while (true) {
			d = in.read();
			if (d >= '0' && d <= '9')
				len = len * 10 + (d - '0');
			else if (d == '\n')
				break;
			else
				throw new SSDBException("protocol error. unexpect byte=" + d);
		}
		byte[] data = new byte[len];
		if (len > 0)
			in.read(data);
		d = in.read();
		if (d != '\n')
			throw new SSDBException("protocol error. unexpect byte=" + d);
		return data;
	}
	
	public static void writeBlock(OutputStream out, byte[] data) throws IOException {
		if (data == null)
			data = RawClient.EMPTY_ARG;
		out.write(Integer.toString(data.length).getBytes());
		out.write('\n');
		out.write(data);
		out.write('\n');
	}
	
	public static void sendCmd(OutputStream out, Cmd cmd, byte[] ... vals) throws IOException {
		SSDBs.writeBlock(out, cmd.bytes());
		for (byte[] bs : vals) {
			SSDBs.writeBlock(out, bs);
		}
		out.write('\n');
		out.flush();
	}
	
	public static Respose readResp(InputStream in) throws IOException {
		Respose resp = new Respose();
		byte[] data = SSDBs.readBlock(in);
		if (data == null)
			throw new SSDBException("protocol error. unexpect \\n");
		resp.stat = new String(data);
		resp.datas = new ArrayList<byte[]>();
		while (true) {
			data = SSDBs.readBlock(in);
			if (data == null)
				break;
			resp.datas.add(data);
		}
		return resp;
	}
	
	public static String version() {
		return "7.5.2"; // 20130104 by wendal
	}
}
