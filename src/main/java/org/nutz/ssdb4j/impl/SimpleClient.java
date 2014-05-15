package org.nutz.ssdb4j.impl;

import java.util.List;

import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.ObjectConv;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;

public class SimpleClient implements SSDB {

	protected SSDBStream stream;
	
	protected ObjectConv conv;

	public SimpleClient(SSDBStream stream) {
		this.stream = stream;
		this.conv = DefaultObjectConv.me;
	}

	public SimpleClient(String host, int port, int timeout) {
		stream = new SocketSSDBStream(host, port, timeout);
		this.conv = DefaultObjectConv.me;
	}

	protected byte[] bytes(Object obj) {
		return conv.bytes(obj);
	}

	protected byte[][] bytess(Object... objs) {
		return conv.bytess(objs);
	}
	

	protected final Response req(Cmd cmd, byte[] first, byte[][] lots) {
		byte[][] vals = new byte[lots.length+1][];
		vals[0] = first;
		for (int i = 0; i < lots.length; i++) {
			vals[i+1] = lots[i];
		}
		return req(cmd, vals);
	}
	
	protected Response req(Cmd cmd, byte[] ... vals) {
		return stream.req(cmd, vals);
	}

	@Override
	public SSDB batch() {
		return new BatchClient(stream);
	}

	@Override
	public List<Response> exec() {
		throw new SSDBException("not batch!");
	}
	
	public void setObjectConv(ObjectConv conv) {
		this.conv = conv;
	}
	
	public void setSSDBStream(SSDBStream stream) {
		this.stream = stream;
	}
	
	//----------------------------------------------------------------------------------

	public Response get(Object key) {
		return req(Cmd.get,bytes(key));
	}

	@Override
	public Response set(Object key, Object val) {
		return req(Cmd.set,bytes(key), bytes(val));
	}

	@Override
	public Response setx(Object key, Object val, int ttl) {
		return req(Cmd.setx,bytes(key), bytes(val), (""+ttl).getBytes());
	}

	@Override
	public Response del(Object key) {
		return req(Cmd.del,bytes(key));
	}

	@Override
	public Response incr(Object key, int val) {
		return req(Cmd.incr,bytes(key), (""+val).getBytes());
	}

	@Override
	public Response exists(Object key) {
		return req(Cmd.exists,bytes(key));
	}

	@Override
	public Response keys(Object start, Object end, int limit) {
		return req(Cmd.keys,bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Response multi_set(Object... pairs) {
		return req(Cmd.multi_set,bytess(pairs));
	}

	@Override
	public Response multi_get(Object... keys) {
		return req(Cmd.multi_get,bytess(keys));
	}

	@Override
	public Response multi_del(Object... keys) {
		return req(Cmd.multi_del,bytess(keys));
	}

	@Override
	public Response scan(Object start, Object end, int limit) {
		return req(Cmd.scan,bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Response rscan(Object start, Object end, int limit) {
		return req(Cmd.rscan,bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Response hset(Object key, Object hkey, Object hval) {
		return req(Cmd.hset,bytes(key), bytes(hkey), bytes(hval));
	}

	@Override
	public Response hdel(Object key, Object hkey) {
		return req(Cmd.hdel,bytes(key), bytes(hkey));
	}

	@Override
	public Response hget(Object key, Object hkey) {
		return req(Cmd.hget,bytes(key), bytes(hkey));
	}

	@Override
	public Response hsize(Object key) {
		return req(Cmd.hsize,bytes(key));
	}

	@Override
	public Response hlist(Object key, Object hkey, int limit) {
		return req(Cmd.hlist,bytes(key), bytes(hkey), (""+limit).getBytes());
	}

	@Override
	public Response hincr(Object key, Object hkey, int val) {
		return req(Cmd.hincr,bytes(key), bytes(hkey), (""+val).getBytes());
	}

	@Override
	public Response hscan(Object key, Object start, Object end, int limit) {
		return req(Cmd.hscan,bytes(key), bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Response hrscan(Object key, Object start, Object end, int limit) {
		return req(Cmd.hrscan,bytes(key), bytes(start), bytes(end), (""+limit).getBytes());
	}

	public Response zset(Object key, Object zkey, int score) {
		return req(Cmd.zset,bytes(key), bytes(zkey), (""+score).getBytes());
	}

	@Override
	public Response zget(Object key, Object zkey) {
		return req(Cmd.zget,bytes(key), bytes(zkey));
	}

	@Override
	public Response zdel(Object key, Object zkey) {
		return req(Cmd.zdel,bytes(key), bytes(zkey));
	}

	@Override
	public Response zincr(Object key, Object zkey, int val) {
		return req(Cmd.zincr,bytes(key), bytes(zkey), (""+val).getBytes());
	}

	@Override
	public Response zsize(Object key) {
		return req(Cmd.zsize,bytes(key));
	}
	
	@Override
	public Response zlist(Object zkey_start, Object zkey_end, int limit) {
		return req(Cmd.zlist, bytes(zkey_start), bytes(zkey_end), (""+limit).getBytes());
	}
	
	@Override
	public Response zrank(Object key, Object zkey) {
		return req(Cmd.zrank,bytes(key), bytes(zkey));
	}

	@Override
	public Response zrrank(Object key, Object zkey) {
		return req(Cmd.zrrank, bytes(key), bytes(zkey));
	}

	@Override
	public Response zscan(Object key, Object zkey_start, int score_start, int score_end, int limit) {
		return req(Cmd.zscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), (""+limit).getBytes());
	}

	@Override
	public Response zrscan(Object key, Object zkey_start, int score_start, int score_end, int limit) {
		return req(Cmd.zrscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), (""+limit).getBytes());
	}

	@Override
	public Response qsize(Object key) {
		return req(Cmd.qsize, bytes(key));
	}

	@Override
	public Response qfront(Object key) {
		return req(Cmd.qfront, bytes(key));
	}

	@Override
	public Response qback(Object key) {
		return req(Cmd.qback, bytes(key));
	}

	@Override
	public Response qpush(Object key, Object value) {
		return req(Cmd.qpush, bytes(key), bytes(value));
	}

	@Override
	public Response qpop(Object key) {
		return req(Cmd.qpop, bytes(key));
	}
	
	@Override
	public Response qlist(Object key_start, Object key_end, int limit) {
		return req(Cmd.qlist, bytes(key_start), bytes(key_end), (""+limit).getBytes());
	}

	@Override
	public Response qclear(Object key) {
		return req(Cmd.qclear, bytes(key));
	}

	@Override
	public Response hkeys(Object key, Object start, Object end, int limit) {
		return req(Cmd.hkeys, bytes(key), bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Response hexists(Object key, Object hkey) {
		return req(Cmd.hexists, bytes(key), bytes(hkey));
	}

	@Override
	public Response hclear(Object key) {
		return req(Cmd.hclear, bytes(key));
	}

	@Override
	public Response multi_hget(Object key, Object... hkeys) {
		return req(Cmd.multi_hget, bytes(key), bytess(hkeys));
	}

	@Override
	public Response multi_hset(Object key, Object... pairs) {
		return req(Cmd.multi_hset, bytes(key), bytess(pairs));
	}

	@Override
	public Response multi_hdel(Object key, Object... hkeys) {
		return req(Cmd.multi_hdel, bytes(key), bytess(hkeys));
	}

	@Override
	public Response zexists(Object key, Object zkey) {
		return req(Cmd.zexists, bytes(key), bytes(zkey));
	}

	@Override
	public Response zclear(Object key) {
		return req(Cmd.zclear, bytes(key));
	}

	@Override
	public Response zkeys(Object key, Object zkey_start, int score_start, int score_end, int limit) {
		return req(Cmd.zkeys, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), (""+limit).getBytes());
	}

	@Override
	public Response zrange(Object key, int offset, int limit) {
		return req(Cmd.zrange, bytes(key), (""+offset).getBytes(), (""+limit).getBytes());
	}

	@Override
	public Response zrrange(Object key, int offset, int limit) {
		return req(Cmd.zrrange, bytes(key), (""+offset).getBytes(), (""+limit).getBytes());
	}

	@Override
	public Response multi_zset(Object key, Object... pairs) {
		return req(Cmd.multi_zset, bytes(key), bytess(pairs));
	}

	@Override
	public Response multi_zget(Object key, Object... zkeys) {
		return req(Cmd.multi_zget, bytes(key), bytess(zkeys));
	}

	@Override
	public Response multi_zdel(Object key, Object... zkeys) {
		return req(Cmd.multi_zdel, bytes(key), bytess(zkeys));
	}

	@Override
	public Response flushdb(String type) {
		if (type == null || type.length() == 0) {
			flushdb_kv();
			flushdb_hash();
			flushdb_zset();
			flushdb_queue();
		} else if ("kv".equals(type)) {
			flushdb_kv();
		} else if ("hash".equals(type)) {
			flushdb_hash();
		} else if ("zset".equals(type)) {
			flushdb_zset();
		} else if ("queue".equals(type)) {
			flushdb_queue();
		}else {
			throw new IllegalArgumentException("not such flushdb mode=" + type);
		}
		Response resp = new Response();
		resp.stat = "ok";
		return resp;
	}
	
	protected long flushdb_kv() {
		long count = 0;
		while (true) {
			List<String> keys = keys("", "", 1000).check().listString();
			if (keys.isEmpty())
				return count;
			count += keys.size();
			for (String key : keys) {
				del(key);
			}
		}
	}
	
	protected long flushdb_hash() {
		long count = 0;
		while (true) {
			List<String> keys = hlist("", "", 1000).check().listString();
			if (keys.isEmpty())
				return count;
			count += keys.size();
			for (String key : keys) {
				hclear(key);
			}
		}
	}
	
	protected long flushdb_zset() {
		long count = 0;
		while (true) {
			List<String> keys = zlist("", "", 1000).check().listString();
			if (keys.isEmpty())
				return count;
			count += keys.size();
			for (String key : keys) {
				zclear(key);
			}
		}
	}
	
	protected long flushdb_queue() {
		long count = 0;
		while (true) {
			List<String> keys = qlist("", "", 1000).check().listString();
			if (keys.isEmpty())
				return count;
			count += keys.size();
			for (String key : keys) {
				qclear(key);
			}
		}
	}

	@Override
	public Response info() {
		return req(Cmd.info);
	}

	@Override
	public Response ping() {
		return req(Cmd.ping);
	}
	
	//------------------------------------------

	@Override
	public Response setnx(Object key, Object val) {
		return req(Cmd.setnx, bytes(key), bytes(val));
	}

	@Override
	public Response getset(Object key, Object val) {
		return req(Cmd.getset, bytes(key), bytes(val));
	}

	@Override
	public Response qslice(Object key, int start, int end) {
		return req(Cmd.qslice, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}

	@Override
	public Response qget(Object key, int index) {
		return req(Cmd.qget, bytes(key), (""+index).getBytes());
	}

	@Override
	public Response zcount(Object key, int start, int end) {
		return req(Cmd.zcount, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}

	@Override
	public Response zsum(Object key, int start, int end) {
		return req(Cmd.zsum, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}

	@Override
	public Response zavg(Object key, int start, int end) {
		return req(Cmd.zavg, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}
	
	@Override
	public Response eval(Object key, Object... args) {
		return req(Cmd.eval, bytes(key), bytess(args));
	}
	
	@Override
	public Response evalsha(Object sha1, Object... args) {
		return req(Cmd.evalsha, bytes(sha1), bytess(args));
	}
}
