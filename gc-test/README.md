# Jackson convertValue Performance Test

A benchmark demonstrating that Jackson's `convertValue()` method creates significant GC pressure by instantiating new objects on every call.

## Benchmark Results

### Jackson convertValue
- **Time taken:** 310 ms
- **Memory used:** 138,269 KB
- **Garbage Collections:** 10

### Simple Cast
- **Time taken:** 5 ms
- **Memory used:** 0 KB
- **Garbage Collections:** 1

## Conclusion

Jackson's `convertValue()` is ~**62x slower** and creates substantial memory overhead compared to simple type casting. Each invocation creates new object instances, triggering frequent garbage collection cycles.

**Recommendation:** Avoid using `convertValue()` in performance-critical paths. Use direct casting or manual mapping when possible.

## Running the Benchmark

```bash
mvn clean compile
mvn exec:java -Dexec.mainClass="com.example.Main"
```
