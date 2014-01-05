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
		ssdb = SSDBs.simple();
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
	public void testZset() {
		fail("Not yet implemented");
	}

	@Test
	public void testZget() {
		fail("Not yet implemented");
	}

	@Test
	public void testZdel() {
		fail("Not yet implemented");
	}

	@Test
	public void testZincr() {
		fail("Not yet implemented");
	}

	@Test
	public void testZsize() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrank() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrrank() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testZscan() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrscan() {
		fail("Not yet implemented");
	}

	@Test
	public void testQsize() {
		fail("Not yet implemented");
	}

	@Test
	public void testQfront() {
		fail("Not yet implemented");
	}

	@Test
	public void testQback() {
		fail("Not yet implemented");
	}

	@Test
	public void testQpush() {
		fail("Not yet implemented");
	}

	@Test
	public void testQpop() {
		fail("Not yet implemented");
	}

	@Test
	public void testBytes() {
		fail("Not yet implemented");
	}

	@Test
	public void testBytess() {
		fail("Not yet implemented");
	}

}
