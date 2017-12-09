package com.indeed.proctor.common;

import com.indeed.proctor.common.model.TestMatrixArtifact;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.el.FunctionMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Support class for loading a test matrix artifact from a URL-based JSON file
 *
 * @author jack
 */
public class UrlProctorLoader extends AbstractJsonProctorLoader {
    @Nonnull
    private final URL inputURL;

    public UrlProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final String inputUrl) throws MalformedURLException {
        this(specification, new URL(inputUrl));
    }

    public UrlProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final String inputUrl, FunctionMapper functionMapper) throws MalformedURLException {
        this(specification, new URL(inputUrl), functionMapper);
    }

    public UrlProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final URL inputUrl) {
        super(UrlProctorLoader.class, specification, RuleEvaluator.FUNCTION_MAPPER);
        this.inputURL = inputUrl;
    }

    public UrlProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final URL inputUrl, FunctionMapper functionMapper) {
        super(UrlProctorLoader.class, specification, functionMapper);
        this.inputURL = inputUrl;
    }

    @Nonnull
    @Override
    protected String getSource() {
        return inputURL.toString();
    }

    @Nullable
    @Override
    protected TestMatrixArtifact loadTestMatrix() throws IOException, MissingTestMatrixException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputURL.openStream(),
                StandardCharsets.UTF_8))) {
            return loadJsonTestMatrix(reader);
        }
    }
}
