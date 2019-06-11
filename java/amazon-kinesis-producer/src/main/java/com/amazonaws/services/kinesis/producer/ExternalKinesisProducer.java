package com.amazonaws.services.kinesis.producer;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class ExternalKinesisProducer extends KinesisProducer {
    private ExternalKinesisProducer(KinesisProducerConfiguration configuration, File inPipe, File outPipe) {
        super(configuration, inPipe, outPipe);
    }

    public static IKinesisProducer of(KinesisProducerConfiguration configuration) throws IOException {
        String tempDirectory = configuration.getTempDirectory();
        PipeVisitor visitor = new PipeVisitor();
        Files.walkFileTree(Paths.get(tempDirectory), visitor);

        if (visitor.inPath == null || visitor.outPath == null) {
            throw new IllegalArgumentException("Could not find pipes under " + tempDirectory);
        }

        return new ExternalKinesisProducer(configuration, visitor.inPath.toFile(), visitor.outPath.toFile());
    }

    private static class PipeVisitor extends SimpleFileVisitor<Path> {
        final PathMatcher inPathMatcher = FileSystems.getDefault().getPathMatcher("glob:**/amz-aws-kpl-in-pipe-*");
        final PathMatcher outPathMatcher = FileSystems.getDefault().getPathMatcher("glob:**/amz-aws-kpl-out-pipe-*");
        Path inPath = null;
        Path outPath = null;

        @Override
        public FileVisitResult visitFile(Path path,
                                         BasicFileAttributes attrs) {
            if (inPath == null && inPathMatcher.matches(path)) {
                inPath = path;
            }
            if (outPath == null && outPathMatcher.matches(path)) {
                outPath = path;
            }
            return (outPath != null && inPath != null) ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }
    }
}
