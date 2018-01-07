package jamsesso.meshmap;

import lombok.Value;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.System.err;

public class MeshMapServer implements Runnable, AutoCloseable {
  private final MeshMapCluster cluster;
  private final Node self;
  private MessageHandler messageHandler;
  private volatile boolean started = false;
  private volatile IOException failure = null;
  private ServerSocket serverSocket;

  public MeshMapServer(MeshMapCluster cluster, Node self) {
    this.cluster = cluster;
    this.self = self;
  }

  public void start(MessageHandler messageHandler) throws IOException {
    if (this.messageHandler != null) {
      throw new IllegalStateException("Cannot restart a dead mesh map server");
    }

    this.messageHandler = messageHandler;
    new Thread(this).start();

    // Wait for the server to start.
    while (!started);

    if (failure != null) {
      throw failure;
    }
  }

  public Message message(Node node, Message message) throws IOException {
    try {
      return Retryable.retry(() -> {
        try (Socket socket = new Socket()) {
          socket.connect(node.getAddress());

          try (OutputStream outputStream = socket.getOutputStream();
               InputStream inputStream = socket.getInputStream()) {
            message.write(outputStream);
            outputStream.flush();
            return Message.read(inputStream);
          }
        }
      }).on(IOException.class).times(3);
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Map<Node, Message> broadcast(Message message) {
    return cluster.getAllNodes().parallelStream()
      .filter(node -> !node.equals(self))
      .map(node -> {
        try {
          return new BroadcastResponse(node, message(node, message));
        }
        catch(IOException e) {
          // TODO Better error handling strategy needed.
          err.println("Unable to broadcast message to node: " + node);
          e.printStackTrace();

          return new BroadcastResponse(node, Message.ERR);
        }
      })
      .collect(Collectors.toMap(BroadcastResponse::getNode, BroadcastResponse::getResponse));
  }

  @Override
  public void run() {
    try {
      serverSocket = new ServerSocket(self.getAddress().getPort());
    }
    catch (IOException e) {
      failure = e;
    }
    finally {
      started = true;
    }

    while (!serverSocket.isClosed()) {
      try (Socket socket = serverSocket.accept();
           InputStream inputStream = socket.getInputStream();
           OutputStream outputStream = socket.getOutputStream()) {
        Message message = Message.read(inputStream);
        Message response = messageHandler.handle(message);

        if(response == null) {
          response = Message.ACK;
        }

        response.write(outputStream);
        outputStream.flush();
      }
      catch (SocketException e) {
        // Socket was closed. Nothing to do here. Node is going down.
      }
      catch (IOException e) {
        // TODO Better error handling strategy is needed.
        err.println("Unable to accept connection");
        e.printStackTrace();
      }
    }
  }

  @Override
  public void close() throws Exception {
    serverSocket.close();
  }

  @Value
  private static class BroadcastResponse {
    Node node;
    Message response;
  }
}
