package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    public static final int MAX_LOG_SIZE = 400;

    private Cluster cluster;
    private Session session;
    private String keyspace;
    private final Deque<String> executedRequests = new ArrayDeque<>();

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IllegalArgumentException("Missing keyspace argument");

        this.keyspace = args[0];

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
        session.execute("USE " + keyspace + ";");

        session.execute("CREATE TABLE IF NOT EXISTS default_table (" +
                "key int PRIMARY KEY, " +
                "value int);");
    }

    @Override
    public boolean execute(Request request, boolean b) {
        return execute(request);
    }

    @Override
    public boolean execute(Request request) {
        if (!(request instanceof RequestPacket)) {
            throw new IllegalArgumentException("Unsupported request type");
        }

        RequestPacket rp = (RequestPacket) request;
        String command = rp.getRequestValue().toString();

        synchronized (executedRequests) {
            if (executedRequests.size() >= MAX_LOG_SIZE) {
                executedRequests.removeFirst();
            }
            executedRequests.addLast(command);
        }

        String[] tokens = command.trim().split("\\s+");
        if (tokens.length < 2) {
            throw new IllegalArgumentException("Malformed command: " + command);
        }

        String op = tokens[0].toLowerCase();
        int key = Integer.parseInt(tokens[1]);
        int value = tokens.length >= 3 ? Integer.parseInt(tokens[2]) : 0;

        switch (op) {
            case "insert":
                session.execute("INSERT INTO default_table (key, value) VALUES (?, ?) IF NOT EXISTS",
                        key, value);
                break;
            case "update":
                session.execute("UPDATE default_table SET value = ? WHERE key = ?",
                        value, key);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: " + op);
        }

        return true;
    }

    @Override
    public String checkpoint(String s) {
        StringBuilder sb = new StringBuilder();
        synchronized (executedRequests) {
            for (String cmd : executedRequests) {
                sb.append(cmd).append("\n");
            }
        }
        return sb.toString();
    }

    @Override
    public boolean restore(String s, String s1) {
        session.execute("TRUNCATE default_table;");

        synchronized (executedRequests) {
            executedRequests.clear();
            if (s1 != null && !s1.isEmpty()) {
                String[] commands = s1.split("\n");
                for (String cmd : commands) {
                    if (!cmd.trim().isEmpty()) {
                        execute(new RequestPacket(cmd, false));
                    }
                }
            }
        }

        return true;
    }

    @Override
    public Request getRequest(String s) throws RequestParseException {
        return new RequestPacket(s, false);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }
}
