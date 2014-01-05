package org.nutz.ssdb4j.spi;

import java.lang.reflect.Method;

public class Cmd {

	public static final Cmd GET = new Cmd("GET", true, true);
	public static final Cmd SET = new Cmd("SET", false, true);
	public static final Cmd SETX = new Cmd("SETX", false, true);
	public static final Cmd DEL = new Cmd("DEL", false, true);
	public static final Cmd INCR = new Cmd("INCR", false, true);
	public static final Cmd EXISTS = new Cmd("EXISTS", true, true);
	public static final Cmd KEYS = new Cmd("KEYS", true, false);
	public static final Cmd MULTI_SET = new Cmd("MULTI_SET", false, false);
	public static final Cmd MULTI_GET = new Cmd("MULTI_GET", true, false);
	public static final Cmd MULTI_DEL = new Cmd("MULTI_DEL", false, false);
	public static final Cmd SCAN = new Cmd("SCAN", false, false);
	public static final Cmd RSCAN = new Cmd("RSCAN", false, false);
	public static final Cmd HSET = new Cmd("HSET", false, true);
	public static final Cmd HDEL = new Cmd("HDEL", false, true);
	public static final Cmd HGET = new Cmd("HGET", true, true);
	public static final Cmd HSIZE = new Cmd("HSIZE", true, true);
	public static final Cmd HLIST = new Cmd("HLIST", false, true);
	public static final Cmd HINCR = new Cmd("HINCR", false, true);
	public static final Cmd HSCAN = new Cmd("HSCAN", false, true);
	public static final Cmd HRSCAN = new Cmd("HRSCAN", false, true);
	public static final Cmd HKEYS = new Cmd("HKEYS", true, true);
	public static final Cmd HEXISTS = new Cmd("HEXISTS", true, true);
	public static final Cmd HCLEAR = new Cmd("HCLEAR", false, true);
	public static final Cmd MULTI_HGET = new Cmd("MULTI_HGET", true, false);
	public static final Cmd MULTI_HSET = new Cmd("MULTI_HSET", false, false);
	public static final Cmd MULTI_HDEL = new Cmd("MULTI_HDEL", false, false);
	public static final Cmd ZSET = new Cmd("ZSET", false, true);
	public static final Cmd ZGET = new Cmd("ZGET", true, true);
	public static final Cmd ZDEL = new Cmd("ZDEL", false, true);
	public static final Cmd ZINCR = new Cmd("ZINCR", false, true);
	public static final Cmd ZSIZE = new Cmd("ZSIZE", true, true);
	public static final Cmd ZRANK = new Cmd("ZRANK", false, true);
	public static final Cmd ZRRANK = new Cmd("ZRRANK", false, true);
	public static final Cmd ZEXISTS = new Cmd("ZEXISTS", true, true);
	public static final Cmd ZCLEAR = new Cmd("ZCLEAR", false, true);
	public static final Cmd ZKEYS = new Cmd("ZKEYS", true, true);
	public static final Cmd ZSCAN = new Cmd("ZSCAN", false, true);
	public static final Cmd ZRSCAN = new Cmd("ZRSCAN", false, true);
	public static final Cmd ZRANGE = new Cmd("ZRANGE", false, true);
	public static final Cmd ZRRANGE = new Cmd("ZRRANGE", false, true);
	public static final Cmd MULTI_ZSET = new Cmd("MULTI_ZSET", false, false);
	public static final Cmd MULTI_ZGET = new Cmd("MULTI_ZGET", true, false);
	public static final Cmd MULTI_ZDEL = new Cmd("MULTI_ZDEL", false, false);
	public static final Cmd QSIZE = new Cmd("QSIZE", true, true);
	public static final Cmd QFRONT = new Cmd("QFRONT", false, true);
	public static final Cmd QBACK = new Cmd("QBACK", false, true);
	public static final Cmd QPUSH = new Cmd("QPUSH", false, true);
	public static final Cmd QPOP = new Cmd("QPOP", false, true);
	public static final Cmd FLUSHDB = new Cmd("FLUSHDB", false, true);
	public static final Cmd INFO = new Cmd("INFO", false, true);
	public static final Cmd PING = new Cmd("PING", false, true);
	//public static final Cmd BATCH = new Cmd("BATCH", false, true);
	//public static final Cmd EXEC = new Cmd("EXEC", false, true);


	protected String name;
	protected byte[] bytes;
	protected boolean slave;
	protected boolean partition;

	public Cmd(String name, boolean slave, boolean partition) {
		super();
		this.name = name;
		this.bytes = name.getBytes();
		this.slave = slave;
		this.partition = partition;
	}

	public byte[] bytes() {
		return bytes;
	}

	public boolean isSlave() {
		return slave;
	}

	public boolean isPartition() {
		return partition;
	}

	public static void main(String[] args) throws Throwable {
		for (Method method :SSDB.class.getMethods()) {
			String cmdName = method.getName().toUpperCase();
			boolean slave = cmdName.contains("GET") || cmdName.contains("SIZE") || cmdName.contains("EXISTS") || cmdName.contains("KEYS");
			boolean partition = cmdName.startsWith("Z") || cmdName.startsWith("H") || 
					(!cmdName.contains("MULTI") && !cmdName.contains("SCAN") && !cmdName.contains("KEYS"));
			System.out.printf("\tpublic static final Cmd %s = new Cmd(\"%s\", %s, %s);\n", cmdName, cmdName, slave, partition);
		}
	}
}
