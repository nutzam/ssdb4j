package org.nutz.ssdb4j.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

public class SimpleClientTest {
	
	SSDB ssdb;

	@Before
	public void init() {
//		ssdb = SSDBs.pool("127.0.0.1", 8888, 2000, null);
//		ssdb = SSDBs.pool("nutzam.com", 8888, 2000, null);
//		ssdb = SSDBs.pool("nutz.cn", 8888, 2000, null);
		ssdb = SSDBs.pool("127.0.0.1", 8888, 2000, null);
//		ssdb = SSDBs.pool("192.168.72.103", 8888, 2000, null);
		Response resp = ssdb.flushdb("");
		assertTrue(resp.ok());
	}
	
	@After
	public void depose() throws IOException {
		ssdb.close();
	}
	
	@Test
	public void test_profiler() {
		ssdb.set("abc", "1234");
		for (int i = 0; i < 10000; i++) {
			ssdb.get("abc");
		}
	}
	
//	@Test
	public void testSimpleLua() throws Throwable {
		ssdb.set("abc", "1");
//		System.out.println(ssdb.get("abc").asString());
////		Respose resp = ssdb.lua("print(ssdb.set('abc', '100000000').values[1]);print(ssdb.get('abc').values[1]);", "0");
//		Respose resp = ssdb.lua("ssdb.set('abc', '100000000');", "0");
//		System.out.println("====================="+ssdb.get("abc").asString());
//		System.out.println(resp.listString());
////		ssdb.lua("demo-lua", "0", "abc");
////		ssdb.lua("print('ABC');", "0");
////		System.out.println(ssdb.get("abc").listString());
//		assertEquals("100000000", ssdb.get("abc").check().asString());
//		
//		
//
//		ssdb.set("demo-lua", "return ssdb.get(ARGV[1]).values[1];");
//		ssdb.set("demo-lua", "return ssdb.get('abc').msg;");
//		Respose resp = ssdb.lua("demo-lua", "0", "abc");
//		System.out.println(resp.listString());
////		assertEquals(100000000, resp.check().asInt());
		//
		//ssdb.eval("ssdb.ping()");
		//
//		ssdb.set("demo-lua", "return ssdb.get(ARGV[1]).values[1];");
//		ExecutorService es = Executors.newFixedThreadPool(128);
		//ssdb.eval("return ssdb.get(ARGV[1]).values[1];", "0", "abc").check();
		//for (int i = 0; i < 10000; i++) {
		//	ssdb.evalsha("4906c10054a9c30fd8be0d0696b62964d5393541", "0", "abc").check();
		//}
		//ssdb.eval("ssdb.set(ARGV[1], 'VVV')", "0", "abc");
		//assertEquals("VVV", ssdb.evalsha("4906c10054a9c30fd8be0d0696b62964d5393541", "0", "abc").check().asString());
		//
		//assertEquals("VVV", ssdb.eval("return redis.call('get', 'abc')", "0").check().asString());
		
//		es.awaitTermination(1, TimeUnit.MINUTES);
	}
	
	@Test
	public void testSimpleClient() {
		assertNotNull(ssdb);
	}


	@Test
	public void test_set_get_del() {
		Response resp = ssdb.set("name", "wendal");
		assertNotNull(resp);
		System.out.println(resp.stat);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		
		resp = ssdb.get("name");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals("wendal", new String(resp.datas.get(0)));
		
		resp = ssdb.del("name");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		
		resp = ssdb.get("name");
		assertNotNull(resp);
		assertFalse(resp.ok());
		assertTrue(resp.notFound());
	}

	@Test
	public void testIncr() {
		Response resp = ssdb.set("age", "28");
		assertNotNull(resp);
		assertTrue(resp.ok());

		ssdb.incr("age", 1);
		ssdb.incr("age", 2);
		ssdb.incr("age", 3);
		
		resp = ssdb.get("age");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals(34, resp.asInt());
	}

	@Test
	public void testMulti_set_del() {
		Response resp = ssdb.multi_del("name", "age");
		ssdb.del("name");
		ssdb.del("age");
		resp = ssdb.multi_set("name", "wendal_multi", "age", "18");
		assertNotNull(resp);
		assertTrue(resp.ok());
		
		resp = ssdb.get("age");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals("18", new String(resp.datas.get(0)));
		

		resp = ssdb.get("name");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals("wendal_multi", new String(resp.datas.get(0)));
		
		resp = ssdb.multi_del("name", "age");
		assertNotNull(resp);
		assertTrue(resp.ok());
		
		resp = ssdb.get("name");
		assertNotNull(resp);
		assertTrue(resp.notFound());
		resp = ssdb.get("age");
		assertNotNull(resp);
		assertTrue(resp.notFound());
	}

	@Test
	public void testScan() {
		for (int i = 0; i < 1000; i++) {
			ssdb.set("key" + i, i);
		}
		Response resp = ssdb.scan("", "", -1);
		assertTrue(resp.ok());
		Map<String, String> values = resp.mapString();
		assertTrue(values.size() >= 1000);
		
		resp = ssdb.scan("", "", 900);
		assertTrue(resp.ok());
		System.out.println(resp.mapString().size());
		assertTrue(resp.mapString().size() <= 900);
	}

	@Test
	public void test_batch() throws InterruptedException {
	    System.out.println(System.currentTimeMillis());
	    for (int i = 0; i < 1000; i++) {
            ssdb.set("aaa" + i, i);
        }
	    System.out.println(System.currentTimeMillis());
		SSDB ssdb = this.ssdb.batch();
		for (int i = 0; i < 1000; i++) {
			ssdb.set("aaa" + i, i);
		}
		System.out.println(System.currentTimeMillis());
		List<Response> resps = ssdb.exec();
		System.out.println(System.currentTimeMillis());
		assertEquals(1000, resps.size());
		for (Response resp : resps) {
			assertTrue(resp.ok());
		}
	}

	@Test
	public void testHset() {
		ssdb.del("my_map");
		ssdb.hset("my_hash", "name", "wendal");
		ssdb.hset("my_hash", "age", 27);
		
		Response resp = ssdb.hget("my_hash", "name");
		assertTrue(resp.ok());
		assertEquals("wendal", resp.asString());
		resp = ssdb.hget("my_hash", "age");
		assertTrue(resp.ok());
		assertEquals(27, resp.asInt());
		
		ssdb.hincr("my_hash", "age", 4);
		resp = ssdb.hget("my_hash", "age");
		assertTrue(resp.ok());
		assertEquals(31, resp.asInt());
		

		resp = ssdb.hsize("my_hash");
		assertTrue(resp.ok());
		assertEquals(2, resp.asInt());

		resp = ssdb.hdel("my_hash", "name");
		assertTrue(resp.ok());
		resp = ssdb.hdel("my_hash", "age");
		assertTrue(resp.ok());
		
		resp = ssdb.hsize("my_hash");
		assertTrue(resp.ok());
		assertEquals(0, resp.asInt());
	}

	@Test
	public void test_info() {
		Response resp = ssdb.info();
		assertTrue(resp.ok());
		for (String str : resp.listString()) {
			System.out.println(str);
		}
	}

	@Test
	public void testZget() {
        ssdb.zclear("wendal");
		ssdb.zset("wendal", "net", 1);
		Response resp = ssdb.zget("wendal", "net");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
	}

	@Test
	public void testZdel() {
        ssdb.zclear("wendal");
		ssdb.zset("wendal", "net", 1);
		Response resp = ssdb.zdel("wendal", "net");
		assertTrue(resp.ok());
		resp = ssdb.zget("wendal", "net");
		assertTrue(resp.notFound());
	}

	@Test
	public void testZincr() {
        ssdb.zclear("wendal");
		ssdb.zset("wendal", "net", 1);
		Response resp = ssdb.zincr("wendal", "net", 10);
		assertTrue(resp.ok());
		assertEquals(11, resp.asInt());
	}

	@Test
	public void testZsize() {
	    ssdb.zclear("wendal");
		ssdb.zset("wendal", "net", 1);
		ssdb.zset("wendal", "net2", 1);
		ssdb.zset("wendal", "ne3", 1);
		ssdb.zset("wendal", "ne8", 1);
		Response resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(4, resp.asInt());
	}

	@Test
	public void testZrank() {
        ssdb.zclear("wendal");
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100);
		}
		Response resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrank("wendal", "net-33");
		assertTrue(resp.ok());
		assertEquals(33, resp.asInt());
	}

	@Test
	public void testZrrank() {
        ssdb.zclear("wendal");
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100);
		}
		Response resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrrank("wendal", "net-33");
		assertTrue(resp.ok());
		assertEquals(100 - 33 - 1, resp.asInt());
	}

	@Test
	public void testZrange() {
        ssdb.zclear("wendal");
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100).check();
		}
		Response resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrange("wendal", 20, 10);
		assertTrue(resp.ok());
		assertEquals(10, resp.map().size());
	}

	@Test
	public void testZrrange() {
        ssdb.zclear("wendal");
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100).check();
		}
		Response resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrrange("wendal", 20, 10);
		assertTrue(resp.ok());
		assertEquals(10, resp.map().size());
	}

	@Test
	public void testZscan() {
        ssdb.zclear("wendal");
		ssdb.zset("wendal", "net", 1);
		ssdb.zset("wendal", "net2", 3);
		ssdb.zset("wendal", "net3", 4);
		Response resp = ssdb.zscan("wendal", "", 1, 2, 2);
		assertTrue(resp.ok());
		assertEquals(1, resp.map().size());
	}

	@Test
	public void testZrscan() {
        ssdb.zclear("wendal");
		ssdb.zset("wendal", "net", 1);
		ssdb.zset("wendal", "net2", 3);
		ssdb.zset("wendal", "net3", 4);
		Response resp = ssdb.zrscan("wendal", "", 7, 1, 2);
		assertTrue(resp.ok());
		assertEquals(2, resp.map().size());
	}

	@Test
	public void testQsize() {
	    ssdb.qclear("qwendal");
		Response resp = ssdb.qpush("qwendal", 1);
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		resp = ssdb.qsize("qwendal");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
	}

	@Test
	public void testQfront() {
        ssdb.qclear("qfront");
		Response resp = ssdb.qpush_front("qfront", "a", "b", "c");
		assertTrue(resp.ok());

		assertEquals("c", ssdb.qpop("qfront").asString());
		assertEquals("b", ssdb.qpop("qfront").asString());
		assertEquals("a", ssdb.qpop("qfront").asString());
	}

	@Test
	public void testQback() {
        ssdb.qclear("qback");
		Response resp = ssdb.qpush_back("qback", "a", "b", "c");
		assertTrue(resp.ok());

		assertEquals("a", ssdb.qpop("qback").asString());
		assertEquals("b", ssdb.qpop("qback").asString());
		assertEquals("c", ssdb.qpop("qback").asString());
	}

	@Test
	public void testQpush() {
        ssdb.qclear("q1");
		Response resp = ssdb.qpush("q1", 123);
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		resp = ssdb.qpush("q1", 4);
		resp = ssdb.qpush("q1", 7);
		resp = ssdb.qpush("q1", 2);
		resp = ssdb.qpush("q1", 1);
		resp = ssdb.qpush("q1", "a", "b", "c");

		assertEquals(123, ssdb.qpop("q1").asInt());
		assertEquals(4, ssdb.qpop("q1").asInt());
		assertEquals(7, ssdb.qpop("q1").asInt());
		assertEquals(2, ssdb.qpop("q1").asInt());
		assertEquals(1, ssdb.qpop("q1").asInt());
		assertEquals("a", ssdb.qpop("q1").asString());
		assertEquals("b", ssdb.qpop("q1").asString());
		assertEquals("c", ssdb.qpop("q1").asString());
	}

	@Test
	public void testSetnx() {
		ssdb.set("abc", "1");
		Response resp = ssdb.setnx("abc", "2");
		assertTrue(resp.ok());
		assertEquals(0, resp.asInt());
		
		resp = ssdb.setnx("abc2", "2");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		System.out.println((char)85);
	}
	
	public static void main(String[] args) throws Exception {
		URL url = new URL("https://github.com/ideawu/ssdb/raw/dev/src/serv.cpp");
		InputStream in = url.openStream();
		Reader reader = new InputStreamReader(in);
		BufferedReader br = new BufferedReader(reader);
		ArrayList<String> ssdb_def_opts = new ArrayList<String>();
		while (true) {
			String line = br.readLine();
			if (line == null)
				break;
//			System.out.println(line);
			line = line.trim();
			if (line.startsWith("#"))
				continue;
			if (!line.startsWith("DEF_PROC"))
				continue;
			ssdb_def_opts.add(line.substring(line.indexOf('(')+1, line.indexOf(')')));
		}
		for (Method method : SSDB.class.getMethods()) {
			ssdb_def_opts.remove(method.getName());
		}
		System.out.println(ssdb_def_opts);
		for (String name : ssdb_def_opts) {
            System.out.printf("    Response %s();\n", name);
        }
		for (String name : ssdb_def_opts) {
            System.out.printf("    public static final Cmd %s = new Cmd(\"%s\", false, false);\n", name, name);
        }
	}
	
//    public static void main(String[] args) throws Throwable {
//    	GenericObjectPool.Config config = new GenericObjectPool.Config();
//        config.lifo = true;
//        config.maxActive = 3;
//        config.maxIdle = 3;
//        config.maxWait = 1000 * 5;
//        config.minEvictableIdleTimeMillis = 1000 * 10;
//        config.minIdle = 3;
//        config.numTestsPerEvictionRun = 100;
//        config.softMinEvictableIdleTimeMillis = 1000 * 10;
//        config.testOnBorrow = true;
//        config.testOnReturn = true;
//        config.testWhileIdle = true;
//        config.timeBetweenEvictionRunsMillis = 1000 * 5;
//        config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
//
//        SSDB ssdb = SSDBs.pool("127.0.0.1", 8888, 1000 * 10, config);
//
//        Response resp = ssdb.ping();
//        System.out.println(resp.stat);
//
//        Thread.sleep(Integer.MAX_VALUE);
//    }
}
