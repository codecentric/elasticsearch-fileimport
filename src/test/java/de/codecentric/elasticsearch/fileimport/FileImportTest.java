package de.codecentric.elasticsearch.fileimport;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileImportTest {
    private static final Logger logger = LogManager.getLogger(FileImportTest.class);
    private Node node;
    private static final int FILE_COUNT = 5010;
    private static final Path TEST_DIR = Paths.get("data", "testfiles");
    private static final Path EMPTY_DIR = Paths.get("data", "empty");

    @Before
    public void before() throws IOException {
        deleteDataDir();
        Files.createDirectories(TEST_DIR);
        Files.createDirectories(EMPTY_DIR);
        for (int i = 0; i < FILE_COUNT; i++) {
            Files.write(TEST_DIR.resolve(Paths.get("jsondoc_" + i + ".json")), "{\"a\":\"b\"}".getBytes("UTF-8"));
        }

        Files.write(TEST_DIR.resolve(Paths.get("jsondoc_bad.json")), "\"a\":\"b\"!!!}".getBytes("UTF-8"));
        Files.write(TEST_DIR.resolve(Paths.get("jsondoc_bad2.json")), "fff}".getBytes("UTF-8"));

        final Settings settings = ImmutableSettings.builder().put("network.host", "127.0.0.1")
                .put("discovery.zen.ping.multicast.enabled", false).build();
        node = NodeBuilder.nodeBuilder().settings(settings).node();
    }

    @After
    public void after() {
        node.close();
    }

    @Test
    public void testNodeImport() throws IOException {
        final Settings settings = ImmutableSettings.builder().loadFromClasspath("file_import_settings_test_1.yml").build();
        final int count = new FileImporter(settings).startAsNode(settings);
        Assert.assertEquals(FILE_COUNT, count);
    }

    @Test
    public void testNodeImportEmpty() throws IOException {
        final Settings settings = ImmutableSettings.builder().loadFromClasspath("file_import_settings_test_5.yml").build();
        final int count = new FileImporter(settings).startAsNode(settings);
        Assert.assertEquals(0, count);
    }

    @Test
    public void testNodeImportFail() throws IOException {
        try {
            final Settings settings = ImmutableSettings.builder().loadFromClasspath("file_import_settings_test_4.yml").build();
            new FileImporter(settings).startAsNode(settings);
            Assert.fail();
        } catch (final ClusterBlockException e) {
            Assert.assertTrue(e.getMessage().contains("no master"));
            // blocked by: [SERVICE_UNAVAILABLE/1/state not recovered /
            // initialized];[SERVICE_UNAVAILABLE/2/no master];
        }
    }

    @Test
    public void testTransportImport() throws IOException {
        final Settings settings = ImmutableSettings.builder().loadFromClasspath("file_import_settings_test_3.yml").build();
        final int count = new FileImporter(settings).startAsTransportClient(settings);
        Assert.assertEquals(FILE_COUNT, count);
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testTransportImportFail() throws IOException {
        final Settings settings = ImmutableSettings.builder().loadFromClasspath("file_import_settings_test_2.yml").build();
        final int count = new FileImporter(settings).startAsTransportClient(settings);
    }

    protected void deleteDataDir() {
        final Path directory = Paths.get("./data");

        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

            });
        } catch (final Exception e) {
            logger.warn(e);
        }
    }
}
