ssdb4j
======

又一个SSDB的Java驱动. 连接池,主从,都有了

SSDB官网
-----------------

https://github.com/ideawu/ssdb

License
-------------------
BSD 3-Clause License

maven
-----------------

```
<dependency>
    <groupId>org.nutz</groupId>
    <artifactId>ssdb4j</artifactId>
    <version>8.8</version>
</dependency>
```

依赖的jar
----------------

Apache Common Pool 1.6 http://commons.apache.org/proper/commons-pool/download_pool.cgi

最简单用法
----------------

```
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.SSDBs;


SSDB ssdb = SSDBs.simple();
ssdb.set("name", "wendal").check(); // call check() to make sure resp is ok 

Response resp = ssdb.get("name");
if (!resp.ok()) {
    // ...
} else {
    log.info("name=" + resp.asString());
}
```
