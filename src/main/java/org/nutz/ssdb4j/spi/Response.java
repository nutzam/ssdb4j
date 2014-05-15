package org.nutz.ssdb4j.spi;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.nutz.ssdb4j.SSDBs;

/**
 * 标准响应,这个类由SSDBStream实例控制生成过程
 * @author wendal(wendal1985@gmail.com)
 *
 */
public class Response {

	public String stat;
	public ArrayList<byte[]> datas = new ArrayList<byte[]>(2);
	public Charset charset = SSDBs.DEFAULT_CHARSET;
	
	public Response check() {
		if (!ok())
			throw new SSDBException("msg=" + stat + ", values=" + listString());
		return this;
	}
	
	public boolean ok() {
		return "ok".equals(stat);
	}

	public boolean notFound() {
		return "not_found".equals(stat);
	}

	protected String _string(byte[] data) {
		return new String(data, charset);
	}
	
	public String asString() {
		return _string(datas.get(0));
	}
	public double asDouble() {
		return Double.parseDouble(asString());
	}
	public int asInt() {
		return Integer.parseInt(asString());
	}
	public long asLong() {
		return Long.parseLong(asString());
	}
	
	public List<String> listString() {
		List<String> list = new ArrayList<String>();
		for (byte[] data : datas) {
			list.add(_string(data));
		}
		return list;
	}
	
	public Map<String, Object> map() {
		if (datas.size() % 2 != 0)
			throw new IllegalArgumentException("not key-value pairs");
		Map<String, Object> map = new HashMap<String, Object>();
		Iterator<byte[]> it = datas.iterator();
		while (it.hasNext()) {
			map.put(_string(it.next()), it.next());
		}
		return map;
	}
	
	public Map<String, String> mapString() {
		if (datas.size() % 2 != 0)
			throw new IllegalArgumentException("not key-value pairs");
		Map<String, String> map = new HashMap<String, String>();
		Iterator<byte[]> it = datas.iterator();
		while (it.hasNext()) {
			map.put(_string(it.next()), _string(it.next()));
		}
		return map;
	}
}
