package org.gautelis.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class RaftTest {
    private static final Logger log = LogManager.getLogger(RaftTest.class);


    @Test
    void addition() {
        assertEquals(2, 2);
    }

}