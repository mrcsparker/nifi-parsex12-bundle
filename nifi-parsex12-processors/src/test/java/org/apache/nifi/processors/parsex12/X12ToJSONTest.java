package org.apache.nifi.processors.parsex12;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class X12ToJSONTest {

    static Logger LOGGER = LoggerFactory.getLogger(X12ToJSONTest.class);

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(new X12ToJSON());
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testProcessor() {
        testRunner.assertValid();
    }

    @Test
    public void testOnTrigger() throws IOException {
        String f = ediFile("/files/sample.835");
        String compare = ediFile("/files/sample.json");

        testRunner.enqueue(Paths.get(f));
        testRunner.run();

        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(X12ToJSON.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());

        MockFlowFile result = results.get(0);

        result.assertContentEquals(Files.readAllBytes(Paths.get(compare)));
    }

    private String ediFile(String fileName) {
        return this.getClass().getResource(fileName).getPath();
    }


}
