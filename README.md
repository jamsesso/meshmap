MeshMap
===

MeshMap is a Java-based implementation of a P2P distributed hash map. Service discovery is provided out of the box, but easy to extend for applications with their own service discovery mechanisms.

The goal of MeshMap is to provide a simple API with all of the functionality of a map data structure with the data distributed across the member nodes in a cluster. MeshMap uses a P2P system (modified ideas borrowed from the Chord algorithm) to work without any master coordination node.

# Example

```java
Node self = new Node("127.0.0.1", 45700);

try (MeshMapCluster cluster = new LocalMeshMapCluster(self, new File("sd"));
     MeshMap<String, Person> people = cluster.join()) {
  Person sam = people.get("Sam"); // Look through the cluster for Sam
  int numPeople = people.size();  // Get the number of entries across all nodes

  // etc... The full java.util.Map API is available!
}
```

With an instance of `MeshMap`, applications can share information as easily as using get/put operations on the map.

# Do you need MeshMap?

If you find yourself needed to iterate over all of the entries in a map, your use-case will probably negate the benefit of using MeshMap.

If you agree with the following statements, MeshMap may be able to help you:

 - I want my data stored in memory at all times for high speed.
 - I have too much data to store in a single node.
 - My data will fit in cluster memory capacity (sum of each node's memory capacity).
 - Nodes need to share data between one another.
 - My query requirements are light and a database is overkill.
 - I do not want to maintain a Redis cluster.
 - If I go to production, I am willing to contribute bug fixes back to MeshMap.

# Service Discovery & Healing

Currently, there is only 1 bundled MeshMap service discovery mechanism available. Contributions for other service discovery mechanisms are welcome!

| Service Discovery Strategy | When to Use? |
|-|-|
| LocalMeshMapCluster | All of the nodes in the cluster share a single filesystem |
| ~~S3MeshMapCluster~~ (TODO) | Nodes are EC2 instances that share visibility to an S3 bucket |

Because data is partitioned across the different nodes in the cluster, when a node joins or leaves the cluster the cluster needs to _heal_ itself. The healing process involves transferring data between the node that is joining or leaving and at most 1 other node in the cluster. When a node leaves the cluster, the data stored locally is transferred to another node determined by MeshMap. When a node joins the cluster, it transfers some of the data from at most 1 other node in the cluster to itself.

# Performance

Performance will mostly be bound by network conditions. I do not currently have any benchmarks to demonstrate.

It is important to mention that it currently takes `O(N)` time to determine which node a map key lives on (where `N` is the number of nodes in the cluster). This **does not** mean that each node is contacted to determine if it contains a key. For example, during a `get` or `put` operation,  only a single network call is made. The complexity for calculating which node a key lives on could be reduced to `O(log N)` in the future, but because typically `N < 25`, the benefits are thought to be negligible.

**Note**: Some of the API calls are significantly more expensive than others.

| API | Network Hits (Worst Case) |
|-|-|
| `size()` | `N-1` |
| `isEmpty()` | `N-1` |
| `containsKey(Object key)` | `1` |
| `containsValue(Object value)` | `N-1` |
| `get(Object k)` | `1` |
| `put(K key, V value)` | `1` |
| `remove(K key)` | `1` |
| `putAll(Map<? extends K, ? extends V> m)` | `m.size()` |
| `clear()` | `N-1` |
| `keySet()` | `N-1` |
| `values()` | `N-1` |
| `entrySet()` | `N-1` |

# Building

MeshMap uses Gradle as a build system and includes the Gradle Wrapper.

```
~$ ./gradlew build
```

# TODO

 - [ ] Implement the `S3MeshMapCluster` service discovery strategy.
 - [ ] Improve `getNodeForKey` calculation algorithm to be `O(log N)`.
 - [ ] Write tests.
 - [ ] Introduce transient nodes: nodes that do not store any data locally.
 - [ ] Provide an API to deterministically calculate cluster ring position.
