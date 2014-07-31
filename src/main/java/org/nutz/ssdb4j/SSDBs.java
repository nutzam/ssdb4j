package org.nutz.ssdb4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.nutz.ssdb4j.impl.SimpleClient;
import org.nutz.ssdb4j.pool.PoolSSDBStream;
import org.nutz.ssdb4j.pool.SocketSSDBStreamPool;
import org.nutz.ssdb4j.replication.ReplicationSSDMStream;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;

/**
 * 封装最常用的SSDB创建方法和协议实现
 * <p></p>数据流: 用户代码--SSDB接口(及ObjectConv接口)--SSDBStream接口--服务器
 * @author wendal(wendal1985@gmail.com)
 *
 */
public class SSDBs {
	
	/**默认主机127.0.0.1*/
	public static String DEFAULT_HOST = "127.0.0.1";
	/**默认端口8888*/
	public static int DEFAULT_PORT = 8888;
	/**默认连接和读写超时2s*/
	public static int DEFAULT_TIMEOUT = 2000;
	
	public static Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
	
	public static final byte[] EMPTY_ARG = new byte[0];

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
		if (config == null) {
			config = new Config();
			config.maxActive = 10;
			config.testWhileIdle = true;
		}
		return new SimpleClient(_pool(host, port, timeout, config));
	}
	
	protected static final PoolSSDBStream _pool(String host, int port, int timeout, Config config) {
		return new PoolSSDBStream(new SocketSSDBStreamPool(host, port, timeout, config));
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
		if (config == null) {
			config = new Config();
			config.maxActive = 10;
			config.testWhileIdle = true;
		}
		PoolSSDBStream master = _pool(masterHost, masterPort, timeout, config);
		PoolSSDBStream slave = _pool(slaveHost, slavePort, timeout, config);
		return new SimpleClient(new ReplicationSSDMStream(master, slave));
	}
	
	/**
	 * 
	 * 从流中读取一个块(ssdb通信协议中定义的Block)
	 * <p></p><b>如果本方法抛异常,应立即关闭输入流</b>
	 * @param in 输入流
	 * @return 如果首字节为回车(\n),则返回null,否则返回Block的data部分
	 * @throws IOException   常规的IO异常
	 * @throws SSDBException 读取到非预期值的时候抛错协议错误
	 */
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
		if (len > 0) {
			int count = 0;
			int r = 0;
			while (count < len) {
				r = in.read(data, count, len - count > 8192 ? 8192 : len - count);
				if (r > 0) {
					count += r;
				} else if (r == -1)
					throw new SSDBException("protocol error. unexpect stream end!");
			}
		}
		d = in.read();
		if (d != '\n')
			throw new SSDBException("protocol error. unexpect byte=" + d);
		return data;
	}
	
	/**
	 * 按ssdb的通信协议写入一个Block
	 * <p></p><b>如果本方法抛异常,应立即关闭输出流</b>
	 * @param out 输出流
	 * @param data 需要写入数据块, 如果为null,则转为长度为0的byte[]
	 * @throws IOException 常规IO异常
	 */
	public static void writeBlock(OutputStream out, byte[] data) throws IOException {
		if (data == null)
			data = EMPTY_ARG;
		out.write(Integer.toString(data.length).getBytes());
		out.write('\n');
		out.write(data);
		out.write('\n');
	}
	
	/**
	 * 向输出流发送一个命令及其参数
	 * <p></p><b>如果本方法抛异常,应立即关闭输出流</b>
	 * @param out 输出流
	 * @param cmd 命令类型
	 * @param vals 命令的参数
	 * @throws IOException 常规IO异常
	 */
	public static void sendCmd(OutputStream out, Cmd cmd, byte[] ... vals) throws IOException {
		SSDBs.writeBlock(out, cmd.bytes());
		for (byte[] bs : vals) {
			SSDBs.writeBlock(out, bs);
		}
		out.write('\n');
		out.flush();
	}
	
	/**
	 * 从输入流读取一个响应
	 * <p></p><b>如果本方法抛异常,应立即关闭输入流</b>
	 * @param in 输入流
	 * @return ssdb标准响应
	 * @throws IOException 常规IO异常
	 * @throws SSDBException 读取到非预期值的时候抛错协议错误
	 */
	public static Response readResp(InputStream in) throws IOException {
		Response resp = new Response();
		byte[] data = SSDBs.readBlock(in);
		if (data == null)
			throw new SSDBException("protocol error. unexpect \\n");
		resp.stat = new String(data);
		while (true) {
			data = SSDBs.readBlock(in);
			if (data == null)
				break;
			resp.datas.add(data);
		}
		return resp;
	}
	
	public static void sendRedisCmd(OutputStream out, Cmd cmd, byte[] ... vals) throws IOException {
		throw new RuntimeException();
	}
	
	public static Response readRedisResp(InputStream in) throws IOException {
		throw new RuntimeException();
	}
	
	/**
	 * 版本号
	 * @return 版本号
	 */
	public static String version() {
		return "8.8";
	}
}
