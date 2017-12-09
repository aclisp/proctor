package com.indeed.proctor.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.indeed.proctor.common.model.TestMatrixArtifact;
import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.el.FunctionMapper;
import java.io.IOException;
import java.io.Reader;

public abstract class AbstractJsonProctorLoader extends AbstractProctorLoader {
    private static final Logger LOGGER = Logger.getLogger(FileProctorLoader.class);

    @Nonnull
    private final ObjectMapper objectMapper = Serializers.lenient();
    @Nullable
    private String fileContents = null;

    public AbstractJsonProctorLoader(@Nonnull final Class<?> cls, @Nonnull final ProctorSpecification specification, @Nonnull final FunctionMapper functionMapper) {
        super(cls, specification, functionMapper);

        final ProctorLoaderDetail detailObject = new ProctorLoaderDetail();
        VarExporter.forNamespace(detailObject.getClass().getSimpleName()).export(detailObject, "");
    }

    @Nullable
    protected TestMatrixArtifact loadJsonTestMatrix(@Nonnull final Reader reader) throws IOException {
        try {
            final TestMatrixArtifact testMatrix = objectMapper.readValue(reader, TestMatrixArtifact.class);
            return testMatrix;
        } catch (@Nonnull final IOException e) {
            LOGGER.error("Unable to load test matrix from " + getSource(), e);
            throw e;
        }
    }

    @Nullable
    public String getFileContents() {
        return fileContents;
    }

    /* class ProctorLoaderDetail is public so VarExporter works correctly */
    public class ProctorLoaderDetail {
        @Export(name = "file-source")
        public String getFileSource() {
            return getSource();
        }

        @Nullable
        @Export(name = "file-contents", doc = "The file contents of a recent successful load. If the file contains invalid JSON, the file contents will not be set.")
        public String getLastFileContents() {
            return getFileContents();
        }
    }
}
