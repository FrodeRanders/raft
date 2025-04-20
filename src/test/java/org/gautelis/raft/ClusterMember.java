package org.gautelis.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Message;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.utilities.BasicAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClusterMember extends BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(ClusterMember.class);

    private final ObjectMapper mapper = new ObjectMapper();

    public ClusterMember(long timeoutMillis, Peer me, List<Peer> peers) {
        super(timeoutMillis, me, peers);
    }

    @Override
    public void handleLogEntry(long myTerm, LogEntry logEntry) {
        // This should be a LogEntry.Type.COMMAND
        log.debug(
                "Received {} log entry (their term {} {} my term {})",
                logEntry.getType(), logEntry.getTerm(), myTerm == logEntry.getTerm() ? "==" : "!=", myTerm
        );
    }

    @Override
    public void handleMessage(String correlationId, String type, JsonNode node, ChannelHandlerContext ctx) throws JsonProcessingException {
        switch (type) {
            case "ClusterCommand" -> {
                ClusterCommand command = mapper.treeToValue(node, ClusterCommand.class);
                log.info("Received cluster message {}", command.getCommand());
            }
        }
    }

    public void inform(String message) {
        final String type = "ClusterCommand";
        ClusterCommand command = new ClusterCommand(
                LogEntry.Type.COMMAND, stateMachine.getTerm(), stateMachine.getId(), message
        );

        try {
            Message msg = new Message(type, command);
            String json = mapper.writeValueAsString(msg);

            stateMachine.getRaftClient().broadcast(type, stateMachine.getTerm(), json);

        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }
}
