package jamsesso.meshmap;

import lombok.Value;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MeshMapImpl<K, V> implements MeshMap<K, V>, MessageHandler {
  private static final String TYPE_PUT = "PUT";
  private static final String TYPE_GET = "GET";
  private static final String TYPE_REMOVE = "REMOVE";
  private static final String TYPE_CLEAR = "CLEAR";
  private static final String TYPE_KEY_SET = "KEY_SET";
  private static final String TYPE_SIZE = "SIZE";
  private static final String TYPE_CONTAINS_KEY = "CONTAINS_KEY";
  private static final String TYPE_CONTAINS_VALUE = "CONTAINS_VALUE";
  private static final String TYPE_DUMP_ENTRIES = "DUMP_ENTRIES";

  private final CachedMeshMapCluster cluster;
  private final MeshMapServer server;
  private final Node self;
  private final Map<Object, Object> delegate;

  public MeshMapImpl(MeshMapCluster cluster, MeshMapServer server, Node self) {
    this.cluster = new CachedMeshMapCluster(cluster);
    this.server = server;
    this.self = self;
    this.delegate = new ConcurrentHashMap<>();
  }

  @Override
  public Message handle(Message message) {
    switch (message.getType()) {
      case Message.TYPE_HI:
      case Message.TYPE_BYE: {
        cluster.clearCache();
        return Message.ACK;
      }

      case TYPE_GET: {
        Object key = message.getPayload(Object.class);
        return new Message(TYPE_GET, delegate.get(key));
      }

      case TYPE_PUT: {
        Entry entry = message.getPayload(Entry.class);
        delegate.put(entry.getKey(), entry.getValue());
        return Message.ACK;
      }

      case TYPE_REMOVE: {
        Object key = message.getPayload(Object.class);
        return new Message(TYPE_REMOVE, delegate.remove(key));
      }

      case TYPE_CLEAR: {
        delegate.clear();
        return Message.ACK;
      }

      case TYPE_KEY_SET: {
        Object[] keys = delegate.keySet().toArray();
        return new Message(TYPE_KEY_SET, keys);
      }

      case TYPE_SIZE: {
        return new Message(TYPE_SIZE, ByteBuffer.allocate(4).putInt(delegate.size()).array());
      }

      case TYPE_CONTAINS_KEY: {
        Object key = message.getPayload(Object.class);
        return delegate.containsKey(key) ? Message.YES : Message.NO;
      }

      case TYPE_CONTAINS_VALUE: {
        Object value = message.getPayload(Object.class);
        return delegate.containsValue(value) ? Message.YES : Message.NO;
      }

      case TYPE_DUMP_ENTRIES: {
        Entry[] entries = delegate.entrySet().stream()
          .map(entry -> new Entry(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList())
          .toArray(new Entry[0]);

        return new Message(TYPE_DUMP_ENTRIES, entries);
      }

      default: {
        return Message.ACK;
      }
    }
  }

  @Override
  public int size() {
    Message sizeMsg = new Message(TYPE_SIZE);

    return delegate.size() + server.broadcast(sizeMsg).entrySet().stream()
      .map(Map.Entry::getValue)
      .filter(response -> TYPE_SIZE.equals(response.getType()))
      .mapToInt(Message::getPayloadAsInt)
      .sum();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    Node target = getNodeForKey(key);

    if (target.equals(self)) {
      // Key lives on the current node.
      return delegate.containsKey(key);
    }

    Message containsKeyMsg = new Message(TYPE_CONTAINS_KEY, key);
    Message response;

    try {
      response = server.message(target, containsKeyMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    return Message.YES.equals(response);
  }

  @Override
  public boolean containsValue(Object value) {
    if (delegate.containsValue(value)) {
      // Check locally first.
      return true;
    }

    Message containsValueMsg = new Message(TYPE_CONTAINS_VALUE, value);

    return server.broadcast(containsValueMsg).entrySet().stream()
      .map(Map.Entry::getValue)
      .anyMatch(Message.YES::equals);
  }

  @Override
  public V get(Object key) {
    return (V) get(key, getNodeForKey(key));
  }

  @Override
  public V put(K key, V value) {
    put(key, value, getNodeForKey(key));
    return value;
  }

  @Override
  public V remove(Object key) {
    return (V) remove(key, getNodeForKey(key));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.entrySet().parallelStream().forEach(entry -> put(entry.getKey(), entry.getValue()));
  }

  @Override
  public void clear() {
    Message clearMsg = new Message(TYPE_CLEAR);
    server.broadcast(clearMsg);
    delegate.clear();
  }

  @Override
  public Set<K> keySet() {
    return cluster.getAllNodes().parallelStream()
      .map(this::keySet)
      .flatMap(Stream::of)
      .map(object -> (K) object)
      .collect(Collectors.toSet());
  }

  @Override
  public Collection<V> values() {
    return entrySet().stream()
      .map(Map.Entry::getValue)
      .collect(Collectors.toList());
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    Message dumpEntriesMsg = new Message(TYPE_DUMP_ENTRIES);
    Set<Map.Entry<K, V>> entries = new HashSet<>();

    for (Map.Entry<Object, Object> localEntry : delegate.entrySet()) {
      entries.add(new TypedEntry<>((K) localEntry.getKey(), (V) localEntry.getValue()));
    }

    for (Map.Entry<Node, Message> response : server.broadcast(dumpEntriesMsg).entrySet()) {
      Entry[] remoteEntries = response.getValue().getPayload(Entry[].class);

      for (Entry remoteEntry : remoteEntries) {
        entries.add(new TypedEntry<>((K) remoteEntry.getKey(), (V) remoteEntry.getValue()));
      }
    }

    return entries;
  }

  @Override
  public String toString() {
    return "MeshMapImpl(Local)[" + String.join(", ", delegate.entrySet().stream()
      .map(entry -> entry.getKey() + ":" + entry.getValue())
      .collect(Collectors.toList()).toArray(new String[0])) + "]";
  }

  public void open() throws MeshMapException {
    Node successor = getSuccessorNode();

    // If there is no successor, there is nothing to do.
    if (successor == null) {
      return;
    }

    // Ask the successor for their key set.
    Object[] keySet = keySet(successor);

    // Transfer the keys from the successor node that should live on this node.
    List<Object> keysToTransfer = Stream.of(keySet)
      .filter(key -> {
        int hash = key.hashCode() & Integer.MAX_VALUE;

        if (self.getId() > successor.getId()) {
          // The successor is the first node (circular node list)
          return hash <= self.getId() && hash > successor.getId();
        }

        return hash <= self.getId();
      })
      .collect(Collectors.toList());

    // Store the values on the current node.
    keysToTransfer.forEach(key -> delegate.put(key, get(key, successor)));

    // Delete the keys from the remote node now that the keys are transferred.
    keysToTransfer.forEach(key -> remove(key, successor));
  }

  @Override
  public void close() throws Exception {
    Node successor = getSuccessorNode();

    // If there is no successor, there is nothing to do.
    if (successor == null) {
      return;
    }

    // Transfer the data from this node to the successor node.
    delegate.forEach((key, value) -> put(key, value, successor));
  }

  private Node getNodeForKey(Object key) {
    int hash = key.hashCode() & Integer.MAX_VALUE;
    List<Node> nodes = cluster.getAllNodes();

    for (Node node : nodes) {
      if (hash <= node.getId()) {
        return node;
      }
    }

    return nodes.get(0);
  }

  private Node getSuccessorNode() {
    List<Node> nodes = cluster.getAllNodes();

    if (nodes.size() <= 1) {
      return null;
    }

    int selfIndex = Collections.binarySearch(nodes, self, Comparator.comparingInt(Node::getId));
    int successorIndex = selfIndex + 1;

    // Find the successor node.
    if (successorIndex > nodes.size() - 1) {
      return nodes.get(0);
    }
    else {
      return nodes.get(successorIndex);
    }
  }

  private Object get(Object key, Node target) {
    if (target.equals(self)) {
      // Value is stored on the local server.
      return delegate.get(key);
    }

    Message getMsg = new Message(TYPE_GET, key);
    Message response;

    try {
      response = server.message(target, getMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!TYPE_GET.equals(response.getType())) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return response.getPayload(Object.class);
  }

  private Object put(Object key, Object value, Node target) {
    if (target.equals(self)) {
      // Value is stored on the local server.
      return delegate.put(key, value);
    }

    Message putMsg = new Message(TYPE_PUT, new Entry(key, value));
    Message response;

    try {
      response = server.message(target, putMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!Message.ACK.equals(response)) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return value;
  }

  private Object remove(Object key, Node target) {
    if (target.equals(self)) {
      // Value is stored on the local server.
      return delegate.remove(key);
    }

    Message removeMsg = new Message(TYPE_REMOVE, key);
    Message response;

    try {
      response = server.message(target, removeMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!TYPE_REMOVE.equals(response.getType())) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return response.getPayload(Object.class);
  }

  private Object[] keySet(Node target) {
    if (target.equals(self)) {
      // Key is on local server.
      return delegate.keySet().toArray();
    }

    Message keySetMsg = new Message(TYPE_KEY_SET);

    try {
      Message response = server.message(target, keySetMsg);
      return response.getPayload(Object[].class);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }
  }

  @Value
  private static class Entry implements Serializable {
    Object key;
    Object value;
  }

  @Value
  private static class TypedEntry<K, V> implements Map.Entry<K, V> {
    K key;
    V value;

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }
}
