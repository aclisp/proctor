package com.indeed.proctor.common.server;

import com.google.common.collect.Maps;
import com.indeed.proctor.common.*;
import com.indeed.proctor.common.model.ConsumableTestDefinition;
import com.indeed.proctor.common.model.TestBucket;
import com.indeed.proctor.common.model.TestType;
import com.indeed.util.core.ReleaseVersion;

import javax.el.FunctionMapper;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class LoadAndRunOnce {
    public static void main(final String[] args) {
        System.out.println("currentPath = " +
            Paths.get(".").toAbsolutePath().normalize().toString());

        final String specPath = "proctor-data/proctor-spec.json";
        final String definitionPath = "proctor-data/proctor-definition.json";

        final ProctorSpecification spec = ProctorUtils.readSpecification(new File(specPath));
        final FunctionMapper functionMapper = RuleEvaluator.defaultFunctionMapperBuilder().build();

        final FileProctorLoader loader = new FileProctorLoader(spec, definitionPath, functionMapper);
        try {
            final Proctor proctor = loader.doLoad();
            System.out.println("------ Proctor Loaded ------");
            proctor.appendTestMatrix(new PrintWriter(System.out));
            System.out.println();
            System.out.println("------ -------------- ------");

            System.out.println("------ Proctor Spec ------");
            System.out.println("providedContext = " + spec.getProvidedContext());
            Map<String, TestSpecification> testSpecificationMap = spec.getTests();
            for (Map.Entry<String, TestSpecification> entry : testSpecificationMap.entrySet()) {
                System.out.print("test " + entry.getKey() + " = ");
                ProctorUtils.serializeTestSpecification(new PrintWriter(System.out), entry.getValue());
            }
            System.out.println();
            System.out.println("------ ------------ ------");

            final String userId = "23";
            final Identifiers identifiers = Identifiers.of(TestType.ANONYMOUS_USER, userId);

            final Map<String, Object> inputContext = Maps.newHashMap();
            inputContext.put("browser", "IE");
            inputContext.put("lang", "en");
            inputContext.put("version", ReleaseVersion.fromString("1.1.0.0"));

            final Map<String, Integer> forcedGroups = Collections.<String, Integer>emptyMap();

            final Collection<String> testNameFilter = Collections.<String>emptyList();

            ProctorResult result = proctor.determineTestGroups(identifiers, inputContext, forcedGroups, testNameFilter);

            System.out.println("------ Proctor Result ------");
            Map<String, TestBucket> buckets = result.getBuckets();
            for (final Map.Entry<String, TestBucket> entry : buckets.entrySet()) {
                System.out.print("testName = " + entry.getKey() + ", ");
                System.out.println("testBucket = " + entry.getValue());
            }

            Map<String, ConsumableTestDefinition> definitionMap = result.getTestDefinitions();
            for (final Map.Entry<String, ConsumableTestDefinition> entry : definitionMap.entrySet()) {
                System.out.print("testName = " + entry.getKey() + ", ");
                System.out.println("testDefinition = " + entry.getValue());
            }

            System.out.println("testVersions = " + result.getTestVersions());
            System.out.println("matrixVersion = " + result.getMatrixVersion());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (MissingTestMatrixException e) {
            e.printStackTrace();
        }
    }
}
