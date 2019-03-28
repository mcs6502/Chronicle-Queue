package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ChronicleQueueDataRemovalTest {

    @Test
    public void test() throws IOException {
        Path path = Files.createTempDirectory(ChronicleQueueDataRemovalTest.class.getSimpleName());

        //noinspection EmptyTryBlock,unused
        try (SingleChronicleQueue cq = SingleChronicleQueueBuilder.binary(path).build()) {
        }

        File dir = path.toFile();
        DirectoryUtils.deleteDir(dir);

        if (dir.exists()) {
            Assert.fail("Folder was not deleted: " + dir.getAbsolutePath());
        }
    }

}
