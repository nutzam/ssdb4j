package org.nutz.ssdb4j.impl;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Respose;
import org.nutz.ssdb4j.spi.SSDB;

public class SimpleClientTest {
	
	SSDB ssdb;

	@Before
	public void init() {
		ssdb = SSDBs.pool("127.0.0.1", 8888, 2000, null);
//		ssdb = SSDBs.pool("nutzam.com", 8888, 2000, null);
//		ssdb = SSDBs.pool("nutz.cn", 8888, 2000, null);
		Respose resp = ssdb.flushdb("");
		assertTrue(resp.ok());
	}
	
	@Test
	public void testSimpleClient() {
		assertNotNull(ssdb);
	}


	@Test
	public void test_set_get_del() {
		Respose resp = ssdb.set("name", "wendal");
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
		Respose resp = ssdb.set("age", "28");
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
		Respose resp = ssdb.multi_del("name", "age");
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
		Respose resp = ssdb.scan("", "", -1);
		assertTrue(resp.ok());
		Map<String, String> values = resp.mapString();
		assertTrue(values.size() >= 1000);
		
		resp = ssdb.scan("", "", 900);
		assertTrue(resp.ok());
		System.out.println(resp.mapString().size());
		assertTrue(resp.mapString().size() <= 900);
	}

	@Test
	public void test_batch() {
		SSDB ssdb = this.ssdb.batch();
		for (int i = 0; i < 1000; i++) {
			ssdb.set("aaa" + i, i);
		}
		System.out.println(System.currentTimeMillis());
		List<Respose> resps = ssdb.exec();
		System.out.println(System.currentTimeMillis());
		assertEquals(1000, resps.size());
		for (Respose resp : resps) {
			assertTrue(resp.ok());
		}
	}

	@Test
	public void testHset() {
		ssdb.del("my_map");
		ssdb.hset("my_hash", "name", "wendal");
		ssdb.hset("my_hash", "age", 27);
		
		Respose resp = ssdb.hget("my_hash", "name");
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
		Respose resp = ssdb.info();
		assertTrue(resp.ok());
		for (String str : resp.listString()) {
			System.out.println(str);
		}
	}

	@Test
	public void testZget() {
		ssdb.zset("wendal", "net", 1);
		Respose resp = ssdb.zget("wendal", "net");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
	}

	@Test
	public void testZdel() {
		ssdb.zset("wendal", "net", 1);
		Respose resp = ssdb.zdel("wendal", "net");
		assertTrue(resp.ok());
		resp = ssdb.zget("wendal", "net");
		assertTrue(resp.notFound());
	}

	@Test
	public void testZincr() {
		ssdb.zset("wendal", "net", 1);
		Respose resp = ssdb.zincr("wendal", "net", 10);
		assertTrue(resp.ok());
		assertEquals(11, resp.asInt());
	}

	@Test
	public void testZsize() {
		ssdb.zset("wendal", "net", 1);
		ssdb.zset("wendal", "net2", 1);
		ssdb.zset("wendal", "ne3", 1);
		ssdb.zset("wendal", "ne8", 1);
		Respose resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(4, resp.asInt());
	}

	@Test
	public void testZrank() {
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100);
		}
		Respose resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrank("wendal", "net-33");
		assertTrue(resp.ok());
		assertEquals(33, resp.asInt());
	}

	@Test
	public void testZrrank() {
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100);
		}
		Respose resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrrank("wendal", "net-33");
		assertTrue(resp.ok());
		assertEquals(100 - 33 - 1, resp.asInt());
	}

	@Test
	public void testZrange() {
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100);
		}
		Respose resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrange("wendal", 20, 10);
		assertTrue(resp.ok());
		assertEquals(10, resp.map().size());
	}

	@Test
	public void testZrrange() {
		for (int i = 0; i < 100; i++) {
			ssdb.zset("wendal", "net-"+i, i + 100);
		}
		Respose resp = ssdb.zsize("wendal");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = ssdb.zrrange("wendal", 20, 10);
		assertTrue(resp.ok());
		assertEquals(10, resp.map().size());
	}

	@Test
	public void testZscan() {
		ssdb.zset("wendal", "net", 1);
		ssdb.zset("wendal", "net2", 3);
		ssdb.zset("wendal", "net3", 4);
		Respose resp = ssdb.zscan("wendal", "", 1, 2, 2);
		assertTrue(resp.ok());
		assertEquals(1, resp.map().size());
	}

	@Test
	public void testZrscan() {
		ssdb.zset("wendal", "net", 1);
		ssdb.zset("wendal", "net2", 3);
		ssdb.zset("wendal", "net3", 4);
		Respose resp = ssdb.zrscan("wendal", "", 7, 1, 2);
		assertTrue(resp.ok());
		assertEquals(2, resp.map().size());
	}

	@Test
	public void testQsize() {
		Respose resp = ssdb.qpush("qwendal", 1);
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		resp = ssdb.qsize("qwendal");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
	}

	@Test
	public void testQfront() {
//		fail("Not yet implemented");
	}

	@Test
	public void testQback() {
//		fail("Not yet implemented");
	}

	@Test
	public void testQpush() {
		Respose resp = ssdb.qpush("q1", 123);
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		resp = ssdb.qpush("q1", 4);
		resp = ssdb.qpush("q1", 7);
		resp = ssdb.qpush("q1", 2);
		resp = ssdb.qpush("q1", 1);

		assertEquals(123, ssdb.qpop("q1").asInt());
		assertEquals(4, ssdb.qpop("q1").asInt());
		assertEquals(7, ssdb.qpop("q1").asInt());
		assertEquals(2, ssdb.qpop("q1").asInt());
		assertEquals(1, ssdb.qpop("q1").asInt());
	}

	@Test
	public void testSetnx() {
		ssdb.set("abc", "1");
		Respose resp = ssdb.setnx("abc", "2");
		assertTrue(resp.ok());
		assertEquals(0, resp.asInt());
		
		resp = ssdb.setnx("abc2", "2");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		System.out.println((char)85);
	}
	
//	public static void main(String[] args) {
//		Method[] mys = SSDB.class.getMethods();
//		for (Method method : mys) {
//			for (Method originMethod : com.udpwork.ssdb.SSDB.class.getMethods()) {
//				if (originMethod.getName().equals(method.getName())) {
//					if (method.getParameterTypes().length != originMethod.getParameterTypes().length) {
//						System.out.println(">> " + method.getName() + "   " + method.getParameterTypes().length + " <> " + originMethod.getParameterTypes().length);
//					}
//					//System.out.println(method.getParameterTypes().length + "_" + originMethod.getParameterTypes().length);
//				}
//			}
//		}
//	}
}
