package org.nutz.ssdb4j.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
	

	protected Response req(Cmd cmd, byte[] first, byte[][] lots) {
		byte[][] vals = new byte[lots.length+1][];
		vals[0] = first;
		for (int i = 0; i < lots.length; i++) {
			vals[i+1] = lots[i];
		}
		return req(cmd, vals);
	}
	
	public Response req(Cmd cmd, byte[] ... vals) {
		return stream.req(cmd, vals);
	}

	public SSDB batch() {
		return new BatchClient(stream, 60, TimeUnit.SECONDS);
	}
	
	public SSDB batch(int timeout, TimeUnit timeUnit) {
	    return new BatchClient(stream, timeout, timeUnit);
	}

	public List<Response> exec() {
		throw new SSDBException("not batch!");
	}
	
	public void setObjectConv(ObjectConv conv) {
		this.conv = conv;
	}
	
	public void changeObjectConv(ObjectConv conv) {
		this.setObjectConv(conv);
	}
	
	public void setSSDBStream(SSDBStream stream) {
		this.stream = stream;
	}
	
	//----------------------------------------------------------------------------------

	public Response get(Object key) {
		return req(Cmd.get,bytes(key));
	}

	
	public Response set(Object key, Object val) {
		return req(Cmd.set,bytes(key), bytes(val));
	}

	
	public Response setx(Object key, Object val, int ttl) {
		return req(Cmd.setx,bytes(key), bytes(val), Integer.toString(ttl).getBytes());
	}

	
	public Response del(Object key) {
		return req(Cmd.del,bytes(key));
	}

	
	public Response incr(Object key, int val) {
		return req(Cmd.incr,bytes(key), Integer.toString(val).getBytes());
	}

	
	public Response exists(Object key) {
		return req(Cmd.exists,bytes(key));
	}

	
	public Response keys(Object start, Object end, int limit) {
		return req(Cmd.keys,bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}

	
	public Response multi_set(Object... pairs) {
		return req(Cmd.multi_set,bytess(pairs));
	}

	
	public Response multi_get(Object... keys) {
		return req(Cmd.multi_get,bytess(keys));
	}

	
	public Response multi_del(Object... keys) {
		return req(Cmd.multi_del,bytess(keys));
	}

	
	public Response scan(Object start, Object end, int limit) {
		return req(Cmd.scan,bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}

	
	public Response rscan(Object start, Object end, int limit) {
		return req(Cmd.rscan,bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}

	
	public Response hset(Object key, Object hkey, Object hval) {
		return req(Cmd.hset,bytes(key), bytes(hkey), bytes(hval));
	}

	
	public Response hdel(Object key, Object hkey) {
		return req(Cmd.hdel,bytes(key), bytes(hkey));
	}

	
	public Response hget(Object key, Object hkey) {
		return req(Cmd.hget,bytes(key), bytes(hkey));
	}

	
	public Response hsize(Object key) {
		return req(Cmd.hsize,bytes(key));
	}

	
	public Response hlist(Object key, Object hkey, int limit) {
		return req(Cmd.hlist,bytes(key), bytes(hkey), Integer.toString(limit).getBytes());
	}

	
	public Response hincr(Object key, Object hkey, int val) {
		return req(Cmd.hincr,bytes(key), bytes(hkey), Integer.toString(val).getBytes());
	}

	
	public Response hscan(Object key, Object start, Object end, int limit) {
		return req(Cmd.hscan,bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}

	
	public Response hrscan(Object key, Object start, Object end, int limit) {
		return req(Cmd.hrscan,bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}

	public Response zset(Object key, Object zkey, long score) {
		return req(Cmd.zset,bytes(key), bytes(zkey), Long.toString(score).getBytes());
	}

	
	public Response zget(Object key, Object zkey) {
		return req(Cmd.zget,bytes(key), bytes(zkey));
	}

	
	public Response zdel(Object key, Object zkey) {
		return req(Cmd.zdel,bytes(key), bytes(zkey));
	}

	
	public Response zincr(Object key, Object zkey, int val) {
		return req(Cmd.zincr,bytes(key), bytes(zkey), Integer.toString(val).getBytes());
	}

	
	public Response zsize(Object key) {
		return req(Cmd.zsize,bytes(key));
	}
	
	
	public Response zlist(Object zkey_start, Object zkey_end, int limit) {
		return req(Cmd.zlist, bytes(zkey_start), bytes(zkey_end), Integer.toString(limit).getBytes());
	}
	
	
	public Response zrank(Object key, Object zkey) {
		return req(Cmd.zrank,bytes(key), bytes(zkey));
	}

	
	public Response zrrank(Object key, Object zkey) {
		return req(Cmd.zrrank, bytes(key), bytes(zkey));
	}

	
	public Response zscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
		return req(Cmd.zscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), Integer.toString(limit).getBytes());
	}

	
	public Response zrscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
		return req(Cmd.zrscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), Integer.toString(limit).getBytes());
	}

	
	public Response qsize(Object key) {
		return req(Cmd.qsize, bytes(key));
	}

	
	public Response qfront(Object key) {
		return req(Cmd.qfront, bytes(key));
	}

	
	public Response qback(Object key) {
		return req(Cmd.qback, bytes(key));
	}

	
	public Response qpush(Object key, Object value) {
		return req(Cmd.qpush, bytes(key), bytes(value));
	}

	
	public Response qpop(Object key) {
		return req(Cmd.qpop, bytes(key));
	}
	
	
	public Response qlist(Object key_start, Object key_end, int limit) {
		return req(Cmd.qlist, bytes(key_start), bytes(key_end), Integer.toString(limit).getBytes());
	}

	
	public Response qclear(Object key) {
		return req(Cmd.qclear, bytes(key));
	}

	
	public Response hkeys(Object key, Object start, Object end, int limit) {
		return req(Cmd.hkeys, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}

	
	public Response hexists(Object key, Object hkey) {
		return req(Cmd.hexists, bytes(key), bytes(hkey));
	}

	
	public Response hclear(Object key) {
		return req(Cmd.hclear, bytes(key));
	}

	
	public Response multi_hget(Object key, Object... hkeys) {
		return req(Cmd.multi_hget, bytes(key), bytess(hkeys));
	}

	
	public Response multi_hset(Object key, Object... pairs) {
		return req(Cmd.multi_hset, bytes(key), bytess(pairs));
	}

	
	public Response multi_hdel(Object key, Object... hkeys) {
		return req(Cmd.multi_hdel, bytes(key), bytess(hkeys));
	}

	
	public Response zexists(Object key, Object zkey) {
		return req(Cmd.zexists, bytes(key), bytes(zkey));
	}

	
	public Response zclear(Object key) {
		return req(Cmd.zclear, bytes(key));
	}

	
	public Response zkeys(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
		return req(Cmd.zkeys, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), Integer.toString(limit).getBytes());
	}

	
	public Response zrange(Object key, int offset, int limit) {
		return req(Cmd.zrange, bytes(key), Integer.toString(offset).getBytes(), Integer.toString(limit).getBytes());
	}

	
	public Response zrrange(Object key, int offset, int limit) {
		return req(Cmd.zrrange, bytes(key), Integer.toString(offset).getBytes(), Integer.toString(limit).getBytes());
	}

	
	public Response multi_zset(Object key, Object... pairs) {
		return req(Cmd.multi_zset, bytes(key), bytess(pairs));
	}

	
	public Response multi_zget(Object key, Object... zkeys) {
		return req(Cmd.multi_zget, bytes(key), bytess(zkeys));
	}

	
	public Response multi_zdel(Object key, Object... zkeys) {
		return req(Cmd.multi_zdel, bytes(key), bytess(zkeys));
	}

	
	public Response flushdb(String type) {
		if (type == null || type.length() == 0) {
			flushdb_kv();
			flushdb_hash();
			flushdb_zset();
			flushdb_list();
		} else if ("kv".equals(type)) {
			flushdb_kv();
		} else if ("hash".equals(type)) {
			flushdb_hash();
		} else if ("zset".equals(type)) {
			flushdb_zset();
		} else if ("list".equals(type)) {
			flushdb_list();
		} else {
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
			multi_del(keys.toArray());
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
	
	protected long flushdb_list() {
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

	
	public Response info() {
		return req(Cmd.info);
	}

	
	public Response ping() {
		return req(Cmd.ping);
	}
	
	//------------------------------------------

	
	public Response setnx(Object key, Object val) {
		return req(Cmd.setnx, bytes(key), bytes(val));
	}

	
	public Response getset(Object key, Object val) {
		return req(Cmd.getset, bytes(key), bytes(val));
	}

	
	public Response qslice(Object key, int start, int end) {
		return req(Cmd.qslice, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
	}

	
	public Response qget(Object key, int index) {
		return req(Cmd.qget, bytes(key), Integer.toString(index).getBytes());
	}

	
	public Response zcount(Object key, int start, int end) {
		return req(Cmd.zcount, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
	}

	
	public Response zsum(Object key, int start, int end) {
		return req(Cmd.zsum, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
	}

	
	public Response zavg(Object key, int start, int end) {
		return req(Cmd.zavg, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
	}
	
	
	public Response eval(Object key, Object... args) {
		return req(Cmd.eval, bytes(key), bytess(args));
	}
	
	
	public Response evalsha(Object sha1, Object... args) {
		return req(Cmd.evalsha, bytes(sha1), bytess(args));
	}
	
	
	public Response ttl(Object key) {
		return req(Cmd.ttl, bytes(key));
	}
	
	
	public Response decr(Object key, int val) {
		return req(Cmd.decr, bytes(key), Integer.toString(val).getBytes());
	}
	
	
	public Response multi_exists(Object... keys) {
		return req(Cmd.multi_exists, bytess(keys));
	}
	
	
	public Response hdecr(Object key, Object hkey, int val) {
		return req(Cmd.hdecr, bytes(key), bytes(hkey), Integer.toString(val).getBytes());
	}
	
	
	public Response hgetall(Object key) {
		return req(Cmd.hgetall, bytes(key));
	}
	
	
	public Response hvals(Object key, Object start, Object end, int limit) {
		return req(Cmd.hvals, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
	}
	
	
	public Response multi_hexists(Object... keys) {
		return req(Cmd.hvals, bytess(keys));
	}
	
	
	public Response multi_hsize(Object... keys) {
		return req(Cmd.multi_hsize, bytes(keys));
	}
	
	
	public Response zdecr(Object key, Object zkey, int val) {
		return req(Cmd.zdecr, bytes(key), bytes(zkey), Integer.toString(val).getBytes());
	}
	
	
	public Response zremrangebyrank(Object key, Object score_start, Object score_end) {
		return req(Cmd.zremrangebyrank, bytes(key), bytes(score_start), bytes(score_end));
	}
	
	
	public Response zremrangebyscore(Object key, Object score_start, Object score_end) {
		return req(Cmd.zremrangebyscore, bytes(key), bytes(score_start), bytes(score_end));
	}
	
	
	public Response multi_zexists(Object key, Object... zkeys) {
		return req(Cmd.zexists, bytes(key), bytess(zkeys));
	}
	
	
	public Response multi_zsize(Object... keys) {
		return req(Cmd.zsize, bytess(keys));
	}
	
	
	public Response qpush_back(Object key, Object value) {
		return req(Cmd.qpush_back, bytes(key), bytes(value));
	}
	
	
	public Response qpush_front(Object key, Object value) {
		return req(Cmd.qpush_front, bytes(key), bytes(value));
	}
	
	
	public Response qpop_back(Object key) {
		return req(Cmd.qpop_back, bytes(key));
	}
	
	
	public Response qpop_front(Object key) {
		return req(Cmd.qpop_front, bytes(key));
	}
	
	
	public Response qrange(Object key, int begin, int limit) {
		return req(Cmd.qrange, bytes(key), Integer.toString(begin).getBytes(), Integer.toString(limit).getBytes());
	}
	
	
	public Response qfix(Object key) {
		return req(Cmd.qfix, bytes(key));
	}
	
	
	public Response dump() {
		return req(Cmd.dump);
	}
	
	
	public Response clear_binlog() {
		return req(Cmd.clear_binlog);
	}
	
	
	public Response compact() {
		return req(Cmd.compact);
	}
	
	
	public Response expire(Object key, int ttl) {
		return req(Cmd.expire, bytes(key), Integer.toString(ttl).getBytes());
	}
	
	
	public Response key_range() {
		return req(Cmd.key_range);
	}
	
	
	public Response sync140() {
		return null;
	}
	
	
	public void close() throws IOException {
		stream.close();
	}

    public Response getbit(Object key, int offset) {
        return req(Cmd.getbit, bytes(key), Integer.toString(offset).getBytes());
    }

    public Response setbit(Object key, int offset, byte on) {
        return req(Cmd.setbit, bytes(key), Integer.toString(offset).getBytes(), on == 1 ? "1".getBytes() : "0".getBytes());
    }

    public Response countbit(Object key, int start, int size) {
        return req(Cmd.countbit, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
    }

    public Response substr(Object key, int start, int size) {
        if (size < 0)
            size = 2000000000;
        return req(Cmd.strlen, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
    }

    public Response getrange(Object key, int start, int size) {
        return req(Cmd.getrange, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
    }

    public Response strlen(Object key) {
        return req(Cmd.strlen, bytes(key));
    }

    public Response redis_bitcount(Object key, int start, int size) {
        return req(Cmd.redis_bitcount, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
    }

    public Response hrlist(Object key, Object hkey, int limit) {
        return req(Cmd.hrlist,bytes(key), bytes(hkey), Integer.toString(limit).getBytes());
    }

    public Response zrlist(Object zkey_start, Object zkey_end, int limit) {
        return req(Cmd.zrlist, bytes(zkey_start), bytes(zkey_end), Integer.toString(limit).getBytes());
    }

    public Response qrlist(Object key_start, Object key_end, int limit) {
        return req(Cmd.qrlist, bytes(key_start), bytes(key_end), Integer.toString(limit).getBytes());
    }

    public Response auth(String passwd) {
        return req(Cmd.auth, bytes(passwd));
    }
    
    public Response qtrim_back(Object key, int size) {
        return req(Cmd.qtrim_back, bytes(key), Integer.toString(size).getBytes());
    }
    
    public Response qtrim_front(Object key, int size) {
        return req(Cmd.qtrim_front, bytes(key), Integer.toString(size).getBytes());
    }
}
