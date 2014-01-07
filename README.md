ssdb4j
======

又一个SSDB的Java驱动

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
    <version>8.0.1</version>
</dependency>
```

依赖的jar
----------------

Apache Common Pool 1.6 

最简单用法
----------------

```
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.SSDBs;


SSDB ssdb = SSDBs.simple();
Respose resp = ssdb.set("name", "wendal");
if (!resp.ok()) {
    // ...
}

resp = ssdb.get("name");
if (!resp.ok()) {
    // ...
}
log.info("name=" + resp.asString());
```
