package com.indeed.proctor.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.indeed.proctor.common.model.TestMatrixArtifact;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;


/**
 * 从远程下载指定格式的实验描述JSON
 */
public class RemoteProctorLoader extends AbstractProctorLoader {
    private static final Logger LOGGER = Logger.getLogger(RemoteProctorLoader.class);

    @Nonnull
    private final URL inputURL = new URL("http://127.0.0.1:3000/proctor/data.json");
    @Nonnull
    private final ObjectMapper objectMapper = Serializers.lenient();
    @Nullable
    private String fileContents = null;

    public static RemoteProctorLoader createInstance() throws MalformedURLException {
        final ProctorSpecification specification = new ProctorSpecification();
        specification.setTests(null);

        return new RemoteProctorLoader(specification);
    }

    public RemoteProctorLoader(@Nonnull final ProctorSpecification specification) throws MalformedURLException {
        super(RemoteProctorLoader.class, specification, RuleEvaluator.FUNCTION_MAPPER);
    }

    @Nullable
    @Override
    protected TestMatrixArtifact loadTestMatrix() throws IOException, MissingTestMatrixException {
        /**
         * 这里根据数据源构造出TestMatrixArtifact，给AbstractProctorLoader做进一步验证。
         */
        final Reader reader = new BufferedReader(new InputStreamReader(inputURL.openStream()));
        try {
            return loadJsonTestMatrix(reader);
        } finally {
            reader.close();
        }
    }

    @Nullable
    protected TestMatrixArtifact loadJsonTestMatrix(@Nonnull final Reader reader) throws IOException {
        final char[] buffer = new char[1024];
        final StringBuilder sb = new StringBuilder();
        while (true) {
            final int read = reader.read(buffer);
            if (read == -1) {
                break;
            }
            if (read > 0) {
                sb.append(buffer, 0, read);
            }
        }
        reader.close();
        final String newContents = sb.toString();
        LOGGER.debug("Load newContents: " + newContents);

        try {
            final TestMatrixArtifact testMatrix = objectMapper.readValue(newContents, TestMatrixArtifact.class);
            if (testMatrix != null) {
                //  record the file contents AFTER successfully loading the matrix
                fileContents = newContents;
            }
            return testMatrix;
        } catch (@Nonnull final JsonParseException e) {
            LOGGER.error("Unable to load test matrix from " + getSource(), e);
            throw e;
        } catch (@Nonnull final JsonMappingException e) {
            LOGGER.error("Unable to load test matrix from " + getSource(), e);
            throw e;
        } catch (@Nonnull final IOException e) {
            LOGGER.error("Unable to load test matrix from " + getSource(), e);
            throw e;
        }
    }

    @Nonnull
    @Override
    protected String getSource() {
        /**
         * 调试用。返回数据源的详细信息。
         */
        return inputURL.toString();
    }

}
