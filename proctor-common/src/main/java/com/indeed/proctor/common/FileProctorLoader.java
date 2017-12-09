package com.indeed.proctor.common;

import com.indeed.proctor.common.model.TestMatrixArtifact;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.el.FunctionMapper;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Support class for loading a test matrix artifact from a JSON file
 *
 * @author ketan
 */
public class FileProctorLoader extends AbstractJsonProctorLoader {
    @Nonnull
    private final File inputFile;

    public FileProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final String inputFile, @Nonnull final FunctionMapper functionMapper) {
        this(specification, new File(inputFile), functionMapper);
    }

    public FileProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final File inputFile, @Nonnull final FunctionMapper functionMapper) {
        super(FileProctorLoader.class, specification, functionMapper);
        this.inputFile = inputFile;
    }

    public FileProctorLoader(@Nonnull final ProctorSpecification specification, @Nonnull final String inputFile) {
        this(specification, new File(inputFile), RuleEvaluator.FUNCTION_MAPPER);
    }

    @Nonnull
    @Override
    protected String getSource() {
        return inputFile.getAbsolutePath();
    }

    @Nullable
    @Override
    protected TestMatrixArtifact loadTestMatrix() throws IOException, MissingTestMatrixException {
        if (!inputFile.exists()) {
            throw new MissingTestMatrixException("File " + inputFile + " does not exist");
        }
        if (!inputFile.canRead()) {
            throw new MissingTestMatrixException("Cannot read input file " + inputFile);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile),
                StandardCharsets.UTF_8))) {
            return loadJsonTestMatrix(reader);
        }
    }
}
