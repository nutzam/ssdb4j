package org.nutz.ssdb4j.spi;

import java.lang.reflect.Method;

public class Cmd {

	public static final Cmd get = new Cmd("get", true, true);
	public static final Cmd set = new Cmd("set", false, true);
	public static final Cmd setx = new Cmd("setx", false, true);
	public static final Cmd del = new Cmd("del", false, true);
	public static final Cmd incr = new Cmd("incr", false, true);
	public static final Cmd exists = new Cmd("exists", true, true);
	public static final Cmd keys = new Cmd("keys", true, false);
	public static final Cmd multi_set = new Cmd("multi_set", false, false);
	public static final Cmd multi_get = new Cmd("multi_get", true, false);
	public static final Cmd multi_del = new Cmd("multi_del", false, false);
	public static final Cmd scan = new Cmd("scan", false, false);
	public static final Cmd rscan = new Cmd("rscan", false, false);
	public static final Cmd hset = new Cmd("hset", false, true);
	public static final Cmd hdel = new Cmd("hdel", false, true);
	public static final Cmd hget = new Cmd("hget", true, true);
	public static final Cmd hsize = new Cmd("hsize", true, true);
	public static final Cmd hlist = new Cmd("hlist", false, true);
	public static final Cmd hincr = new Cmd("hincr", false, true);
	public static final Cmd hscan = new Cmd("hscan", false, true);
	public static final Cmd hrscan = new Cmd("hrscan", false, true);
	public static final Cmd hkeys = new Cmd("hkeys", true, true);
	public static final Cmd hexists = new Cmd("hexists", true, true);
	public static final Cmd hclear = new Cmd("hclear", false, true);
	public static final Cmd multi_hget = new Cmd("multi_hget", true, false);
	public static final Cmd multi_hset = new Cmd("multi_hset", false, false);
	public static final Cmd multi_hdel = new Cmd("multi_hdel", false, false);
	public static final Cmd zset = new Cmd("zset", false, true);
	public static final Cmd zget = new Cmd("zget", true, true);
	public static final Cmd zdel = new Cmd("zdel", false, true);
	public static final Cmd zincr = new Cmd("zincr", false, true);
	public static final Cmd zsize = new Cmd("zsize", true, true);
	public static final Cmd zlist = new Cmd("zlist", true, true);
	public static final Cmd zrank = new Cmd("zrank", false, true);
	public static final Cmd zrrank = new Cmd("zrrank", false, true);
	public static final Cmd zexists = new Cmd("zexists", true, true);
	public static final Cmd zclear = new Cmd("zclear", false, true);
	public static final Cmd zkeys = new Cmd("zkeys", true, true);
	public static final Cmd zscan = new Cmd("zscan", false, true);
	public static final Cmd zrscan = new Cmd("zrscan", false, true);
	public static final Cmd zrange = new Cmd("zrange", false, true);
	public static final Cmd zrrange = new Cmd("zrrange", false, true);
	public static final Cmd multi_zset = new Cmd("multi_zset", false, false);
	public static final Cmd multi_zget = new Cmd("multi_zget", true, false);
	public static final Cmd multi_zdel = new Cmd("multi_zdel", false, false);
	public static final Cmd qsize = new Cmd("qsize", true, true);
	public static final Cmd qfront = new Cmd("qfront", false, true);
	public static final Cmd qback = new Cmd("qback", false, true);
	public static final Cmd qpush = new Cmd("qpush", false, true);
	public static final Cmd qpop = new Cmd("qpop", false, true);
	public static final Cmd qlist = new Cmd("qlist", false, true);
	public static final Cmd qclear = new Cmd("qclear", false, true);
	public static final Cmd flushdb = new Cmd("flushdb", false, true);
	public static final Cmd info = new Cmd("info", false, true);
	public static final Cmd ping = new Cmd("ping", false, true);
//	public static final Cmd batch = new Cmd("batch", false, true);
//	public static final Cmd exec = new Cmd("exec", false, true);


	public static final Cmd setnx = new Cmd("setnx", false, true);
	public static final Cmd getset = new Cmd("getset", false, true);
	public static final Cmd qslice = new Cmd("qslice", true, true);
	public static final Cmd qget = new Cmd("qget", true, true);
	public static final Cmd zcount = new Cmd("zcount", true, true);
	public static final Cmd zsum = new Cmd("zsum", true, true);
	public static final Cmd zavg = new Cmd("zavg", true, true);
	

	public static final Cmd eval = new Cmd("eval", false, false);
	public static final Cmd evalsha = new Cmd("evalsha", false, false);
	
	public static final Cmd ttl = new Cmd("ttl", false, true);
	
	public static final Cmd decr             =  new Cmd("decr"           , true, true);
	public static final Cmd multi_exists     =  new Cmd("multi_exists"   , true, false);
	public static final Cmd hdecr            =  new Cmd("hdecr"          , false, false);
	public static final Cmd hgetall          =  new Cmd("hgetall"        , false, false);
	public static final Cmd hvals            =  new Cmd("hvals"          , false, false);
	public static final Cmd multi_hexists    =  new Cmd("multi_hexists"  , false, false);
	public static final Cmd multi_hsize      =  new Cmd("multi_hsize"    , false, false);
	public static final Cmd zdecr            =  new Cmd("zdecr"          , false, false);
	public static final Cmd zremrangebyrank  =  new Cmd("zremrangebyrank", false, false);
	public static final Cmd zremrangebyscore =  new Cmd("zremrangebyscor", false, false);
	public static final Cmd multi_zexists    =  new Cmd("multi_zexists"  , false, false);
	public static final Cmd multi_zsize      =  new Cmd("multi_zsize"    , false, false);
	public static final Cmd qpush_front      =  new Cmd("qpush_front"    , false, false);
	public static final Cmd qpush_back       =  new Cmd("qpush_back"     , false, false);
	public static final Cmd qpop_front       =  new Cmd("qpop_front"     , false, false);
	public static final Cmd qpop_back        =  new Cmd("qpop_back"      , false, false);
	public static final Cmd qfix             =  new Cmd("qfix"           , false, false);
	public static final Cmd qrange           =  new Cmd("qrange"         , false, false);
	public static final Cmd dump             =  new Cmd("dump"           , false, false);
	public static final Cmd sync140          =  new Cmd("sync140"        , false, false);
	public static final Cmd compact          =  new Cmd("compact"        , false, false);
	public static final Cmd key_range        =  new Cmd("key_range"      , false, false);
	public static final Cmd expire           =  new Cmd("expire"         , false, false);
	public static final Cmd clear_binlog     =  new Cmd("clear_binlog"   , false, false);


    public static final Cmd getbit = new Cmd("getbit", false, false);
    public static final Cmd setbit = new Cmd("setbit", false, false);
    public static final Cmd countbit = new Cmd("countbit", false, false);
    public static final Cmd substr = new Cmd("substr", false, false);
    public static final Cmd getrange = new Cmd("getrange", false, false);
    public static final Cmd strlen = new Cmd("strlen", false, false);
    public static final Cmd redis_bitcount = new Cmd("redis_bitcount", false, false);
    public static final Cmd hrlist = new Cmd("hrlist", false, false);
    public static final Cmd zrlist = new Cmd("zrlist", false, false);
    public static final Cmd qrlist = new Cmd("qrlist", false, false);
    public static final Cmd auth = new Cmd("auth", false, false);

    public static final Cmd qtrim_front = new Cmd("qtrim_front", false, false);
    public static final Cmd qtrim_back = new Cmd("qtrim_back", false, false);
	
	protected String name;
	protected byte[] bytes;
	protected boolean slave;
	protected boolean partition;

	public Cmd(String name, boolean slave, boolean partition) {
		super();
		this.name = name;
		this.bytes = name.toLowerCase().getBytes();
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
			System.out.printf("\tpublic static final Cmd %s = new Cmd(\"%s\", %s, %s);\n", cmdName.toLowerCase(), cmdName.toLowerCase(), slave, partition);
		}
	}
}
