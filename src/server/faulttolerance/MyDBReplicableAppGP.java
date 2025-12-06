package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
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

/**
 * GigaPaxos-based fault-tolerant database application using Cassandra.
 * Implements bounded log checkpointing (Option A).
 */
public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    public static final int MAX_LOG_SIZE = 400;

    private Cluster cluster;
    private Session session;
    private String keyspace;

    // Bounded log of executed requests for checkpointing
    private final Deque<String> executedRequests = new ArrayDeque<>();

    /**
     * Constructor used by GigaPaxos reflection.
     *
     * @param args args[0] = keyspace name
     * @throws IOException
     */
    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IllegalArgumentException("Missing keyspace argument");

        this.keyspace = args[0];

        // Connect to Cassandra on localhost default port
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        // Ensure keyspace exists
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");

        // Use keyspace
        session.execute("USE " + keyspace + ";");

        // Ensure default table exists
        session.execute("CREATE TABLE IF NOT EXISTS default_table (" +
                "key int PRIMARY KEY, " +
                "value int);");
    }

    /**
     * Execute a GigaPaxos request (client-facing version).
     */
    @Override
    public boolean execute(Request request, boolean b) {
        return execute(request);
    }

    /**
     * Execute a GigaPaxos request.
     */
    @Override
    public boolean execute(Request request) {
        if (!(request instanceof RequestPacket)) {
            throw new IllegalArgumentException("Unsupported request type");
        }

        RequestPacket rp = (RequestPacket) request;
        String command = rp.getCommand();

        // Log the command for checkpointing
        synchronized (executedRequests) {
            if (executedRequests.size() >= MAX_LOG_SIZE) {
                executedRequests.removeFirst();
            }
            executedRequests.addLast(command);
        }

        // Parse command: expecting "insert key value" or "update key value"
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

    /**
     * Checkpoint current executed requests into a string.
     */
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

    /**

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
