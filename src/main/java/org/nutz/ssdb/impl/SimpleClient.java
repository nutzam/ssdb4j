package org.nutz.ssdb.impl;

import java.util.Map;
import java.util.Map.Entry;

import org.nutz.ssdb.spi.Respose;
import org.nutz.ssdb.spi.SSDB;
import org.nutz.ssdb.spi.SSDBException;
import org.nutz.ssdb.spi.SSDBStream;

public class SimpleClient implements SSDB {

	public static String DEFAULT_HOST = "127.0.0.1";
	public static int DEFAULT_PORT = 8888;
	public static int DEFAULT_TIMEOUT = 2000;
	
	protected RawClient raw;
	
	public SimpleClient() {
		this(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT);
	}

	public SimpleClient(String host, int port) {
		this(host, port, DEFAULT_TIMEOUT);
	}

	public SimpleClient(SSDBStream stream) {
		this.raw = new RawClient(stream);
	}

	public SimpleClient(String host, int port, int timeout) {
		this.raw = new RawClient(new SocketSSDBStream(host, port, timeout));
	}

	public Respose get(Object key) {
		return raw.get(bytes(key));
	}

	@Override
	public Respose set(Object key, Object val) {
		return raw.set(bytes(key), bytes(val));
	}
	
	@Override
	public Respose setx(Object key, Object val, int ttl) {
		return raw.setx(bytes(key), bytes(val), ttl);
	}

	@Override
	public Respose del(Object key) {
		return raw.del(bytes(key));
	}

	@Override
	public Respose incr(Object key, int val) {
		return raw.incr(bytes(key), val);
	}
	
	@Override
	public Respose exists(Object key) {
		return raw.exists(bytes(key));
	}
	
	@Override
	public Respose keys(Object start, Object end, int limit) {
		return raw.keys(bytes(start), bytes(end), limit);
	}

	@Override
	public Respose multi_set(Object... pairs) {
		return raw.multi_set(bytess(pairs));
	}
	
	@Override
	public Respose multi_get(Object... keys) {
		return raw.multi_get(bytess(keys));
	}

	@Override
	public Respose multi_del(Object... keys) {
		return raw.multi_del(bytess(keys));
	}

	@Override
	public Respose scan(Object start, Object end, int limit) {
		return raw.scan(bytes(start), bytes(end), limit);
	}

	@Override
	public Respose rscan(Object start, Object end, int limit) {
		return raw.rscan(bytes(start), bytes(end), limit);
	}

	@Override
	public Respose hset(Object key, Object hkey, Object hval) {
		return raw.hset(bytes(key), bytes(hkey), bytes(hval));
	}

	@Override
	public Respose hdel(Object key, Object hkey) {
		return raw.hdel(bytes(key), bytes(hkey));
	}

	@Override
	public Respose hget(Object key, Object hkey) {
		return raw.hget(bytes(key), bytes(hkey));
	}

	@Override
	public Respose hsize(Object key) {
		return raw.hsize(bytes(key));
	}

	@Override
	public Respose hlist(Object key, Object hkey, int limit) {
		return raw.hlist(bytes(key), bytes(hkey), limit);
	}

	@Override
	public Respose hincr(Object key, Object hkey, int val) {
		return raw.hincr(bytes(key), bytes(hkey), val);
	}

	@Override
	public Respose hscan(Object key, Object start, Object end, int limit) {
		return raw.hscan(bytes(key), bytes(start), bytes(end), limit);
	}

	@Override
	public Respose hrscan(Object key, Object start, Object end, int limit) {
		return raw.hrscan(bytes(key), bytes(start), bytes(end), limit);
	}

	public Respose zset(Object key, Object zkey, double score) {
		return raw.zset(bytes(key), bytes(zkey), score);
	}

	@Override
	public Respose zget(Object key, Object zkey) {
		return raw.zget(bytes(key), bytes(zkey));
	}

	@Override
	public Respose zdel(Object key, Object zkey) {
		return raw.zdel(bytes(key), bytes(zkey));
	}

	@Override
	public Respose zincr(Object key, Object zkey, int val) {
		return raw.zincr(bytes(key), bytes(zkey), val);
	}

	@Override
	public Respose zsize(Object key) {
		return raw.zsize(bytes(key));
	}

	@Override
	public Respose zrank(Object key, Object zkey) {
		return raw.zrank(bytes(key), bytes(zkey));
	}

	@Override
	public Respose zrrank(Object key, Object zkey) {
		return raw.zrrank(bytes(key), bytes(zkey));
	}

	@Override
	public Respose zscan(Object key, Object start, Object end, int limit) {
		return raw.zscan(bytes(key), bytes(start), bytes(end), limit);
	}

	@Override
	public Respose zrscan(Object key, Object start, Object end, int limit) {
		return raw.zrscan(bytes(key), bytes(start), bytes(end), limit);
	}

	@Override
	public Respose qsize(Object key) {
		return raw.qsize(bytes(key));
	}

	@Override
	public Respose qfront(Object key) {
		return raw.qfront(bytes(key));
	}

	@Override
	public Respose qback(Object key) {
		return raw.qback(bytes(key));
	}

	@Override
	public Respose qpush(Object key, Object value) {
		return raw.qpush(bytes(key), bytes(value));
	}

	@Override
	public Respose qpop(Object key) {
		return raw.qpop(bytes(key));
	}
	
	protected byte[] bytes(Object obj) {
		if (obj == null)
			throw new IllegalArgumentException("arg is null");
		if (obj instanceof byte[])
			return (byte[])obj;
		// TODO 支持输入流作为参数
		// TODO 支持字符集设置
		return obj.toString().getBytes();
	}
	
	@SuppressWarnings("unchecked")
	// TODO 做成插件形式,可配置
	protected byte[][] bytess(Object ... objs) {
		if (objs == null)
			throw new IllegalArgumentException("arg is null");
		if (objs instanceof byte[][])
			return (byte[][])objs;
		if (objs.length == 1) {
			Object arg = objs[0];
			if (arg instanceof Map) {
				Map<Object, Object> map = (Map<Object, Object>)arg;
				byte[][] args = new byte[map.size() * 2][];
				int i = 0;
				for (Entry<Object, Object> en : map.entrySet()) {
					args[i] = bytes(en.getKey());
					args[i+1] = bytes(en.getValue());
					i +=2;
				}
				return args;
			}
		}
		byte[][] args = new byte[objs.length][];
		for (int i = 0; i < args.length; i++) {
			args[i] = bytes(objs[i]);
		}
		// TODO 支持输入流作为参数
		// TODO 支持字符集设置
		return args;
	}

	@Override
	public Respose hkeys(Object key, Object start, Object end, int limit) {
		return raw.hkeys(bytes(key), bytes(start), bytes(end), limit);
	}

	@Override
	public Respose hexists(Object key, Object hkey) {
		return raw.hexists(bytes(key), bytes(hkey));
	}

	@Override
	public Respose hclear(Object key) {
		return raw.hclear(bytes(key));
	}

	@Override
	public Respose multi_hget(Object key, Object... hkeys) {
		return raw.multi_hget(bytes(key), bytess(hkeys));
	}

	@Override
	public Respose multi_hset(Object key, Object... pairs) {
		return raw.multi_hset(bytes(key), bytess(pairs));
	}

	@Override
	public Respose multi_hdel(Object key, Object... hkeys) {
		return raw.multi_hdel(bytes(key), bytess(hkeys));
	}

	@Override
	public Respose zexists(Object key, Object zkey) {
		return raw.zexists(bytes(key), bytes(zkey));
	}

	@Override
	public Respose zclear(Object key) {
		return raw.zclear(bytes(key));
	}

	@Override
	public Respose zkeys(Object key, Object start, Object end, int limit) {
		return raw.zkeys(bytes(key), bytes(start), bytes(end), limit);
	}

	@Override
	public Respose zrange(Object key, int offset, int limit) {
		return raw.zrange(bytes(key), offset, limit);
	}

	@Override
	public Respose zrrange(Object key, int offset, int limit) {
		return raw.zrrange(bytes(key), offset, limit);
	}

	@Override
	public Respose multi_zset(Object key, Object... pairs) {
		return raw.multi_zset(bytes(key), bytess(pairs));
	}

	@Override
	public Respose multi_zget(Object key, Object... zkeys) {
		return raw.multi_zget(bytes(key), bytess(zkeys));
	}

	@Override
	public Respose multi_zdel(Object key, Object... zkeys) {
		return raw.multi_zdel(bytes(key), bytess(zkeys));
	}

	@Override
	public SSDB batch() {
		throw new SSDBException("not impl yet");
	}

	@Override
	public Respose exec() {
		throw new SSDBException("not batch!");
	}
}
