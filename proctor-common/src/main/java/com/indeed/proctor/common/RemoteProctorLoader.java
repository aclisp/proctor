package com.indeed.proctor.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.indeed.proctor.common.admin.model.Test;
import com.indeed.proctor.common.admin.model.TestGroup;
import com.indeed.proctor.common.admin.model.TestMatrix;
import com.indeed.proctor.common.model.*;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


/**
 * 从远程下载指定格式的实验描述JSON
 */
public class RemoteProctorLoader extends AbstractProctorLoader {
    private static final Logger LOGGER = Logger.getLogger(RemoteProctorLoader.class);

    private static final int MAX_ALLOCATION = 10000;

    @Nonnull
    private final URL inputURL = new URL("http://127.0.0.1:3000/proctor/adminModel.json");
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
        //LOGGER.debug("Load newContents: " + newContents);

        try {
            final TestMatrix testMatrix = objectMapper.readValue(newContents, TestMatrix.class);
            if (testMatrix != null) {
                //  record the file contents AFTER successfully loading the matrix
                fileContents = newContents;
            }
            TestMatrixArtifact artifact = createArtifact(testMatrix);
            return artifact;
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

    private void allocateTraffic(@Nonnull final ConsumableTestDefinition testDefinition,
                                 @Nonnull final Test test) {
        List<Range> ranges = testDefinition.getAllocations().get(0).getRanges();
        List<TestBucket> buckets = testDefinition.getBuckets();
        int totalActiveBucket = buckets.size() - 1;

        // 核心算法：利用多轮错位分配的方法，来安排流量分布
        //         第一轮先按offset排好ABC
        //         第二轮再按offset+1排余下的ABC
        //         以此类推...
        // 限制条件：totalActiveBucket在实验开始之后不能改变
        // 变通方法：可以多创建一些分布为0的分组
        Map<TestBucket, Long> remainingRatio = Maps.newHashMap();
        AtomicInteger round = new AtomicInteger(0);
        for (; round.get() < totalActiveBucket; round.incrementAndGet()) {
            buckets.stream().skip(1)  // inactive bucket is always the first one.
                    .forEachOrdered(bucket -> {
                        long bucketRatio;
                        if (!remainingRatio.containsKey(bucket)) {
                            TestGroup testGroup = findTestGroupByVariable(test, bucket.getName());
                            long absoluteRatio = test.getRatio() * testGroup.getRatio();
                            long absoluteTotal = 10000 * 10000;
                            bucketRatio = MAX_ALLOCATION * absoluteRatio / absoluteTotal;
                            remainingRatio.put(bucket, bucketRatio);
                        } else {
                            bucketRatio = remainingRatio.get(bucket);
                        }
                        LOGGER.debug("bucket `" + test.getTestId() + "/" + bucket.getName() + "/" + bucket.getValue() +
                                "` has a ratio of " + bucketRatio + "/" + MAX_ALLOCATION + " at round " + round.get());
                        int bucketIndex = (bucket.getValue() + round.get()) % totalActiveBucket;
                        AtomicInteger count = new AtomicInteger(0);
                        IntStream.range(0, ranges.size())
                                .filter(i -> (i % totalActiveBucket) == bucketIndex)
                                .filter(i -> ranges.get(i).getBucketValue() == -1)
                                .limit(bucketRatio)
                                .forEach(i -> {
                                    ranges.get(i).setBucketValue(bucket.getValue());
                                    count.incrementAndGet();
                                });
                        remainingRatio.put(bucket, bucketRatio - count.get());
                    });
        }

        // 验证
        buckets.stream().skip(1)
                .forEachOrdered(bucket -> {
                    long total = ranges.size();
                    long count = ranges.stream().filter(range -> range.getBucketValue() == bucket.getValue()).count();
                    LOGGER.debug("bucket `" + test.getTestId() + "/" + bucket.getName() + "/" + bucket.getValue() +
                            "` has a ratio of " + count + "/" + total + " after allocation");
                });
    }

    private TestGroup findTestGroupByVariable(Test test, String variable) {
        return test.getTestGroups().stream().filter(testGroup -> testGroup.getVariable().equals(variable))
                .findFirst().get();
    }

    @Nonnull
    private TestMatrixArtifact createArtifact(@Nonnull final TestMatrix testMatrix) {
        TestMatrixArtifact artifact = new TestMatrixArtifact();

        Audit audit = new Audit();
        // Version以所有实验的id的md5为准
        MessageDigest testsDigest = ProctorUtils.createMessageDigest();
        testMatrix.getTests().stream().sorted(Comparator.comparing(Test::getTestId))
                .forEachOrdered(test -> {
                    testsDigest.update(Longs.toByteArray(test.getId()));
                });
        audit.setVersion(Base64.getEncoder().encodeToString(testsDigest.digest()));
        audit.setUpdated(System.currentTimeMillis());
        audit.setUpdatedBy(RemoteProctorLoader.class.getName());
        artifact.setAudit(audit);

        Map<String, ConsumableTestDefinition> testDefinitionMap = Maps.newHashMap();
        testMatrix.getTests().stream().filter(test -> {
            switch (test.getState()) {
                case "running":
                    return true;
                case "paused":
                    return true;
                default:
                    return false;
            }
        }).forEach(test -> {
            ConsumableTestDefinition testDefinition = new ConsumableTestDefinition();
            testDefinition.setSalt(test.getTestId());
            testDefinition.setVersion(test.getTestId() + "." + String.valueOf(test.getId()));
            testDefinition.setDescription("`" + test.getName() + "` " + test.getDescription());
            switch (test.getType()) {
                case "deviceId":
                    testDefinition.setTestType(TestType.DEVICE_ID);
                    break;
                case "uid":
                    testDefinition.setTestType(TestType.USER_ID);
                    break;
                default:
                    testDefinition.setTestType(TestType.RANDOM);
            }

            List<TestBucket> buckets = Lists.newArrayList();
            buckets.add(new TestBucket("inactive", -1, ""));
            // 按TestGroup的variable排序
            AtomicInteger internalBucketId = new AtomicInteger(0);
            test.getTestGroups().stream().sorted(Comparator.comparing(TestGroup::getVariable))
                    .forEachOrdered(testGroup -> {
                        TestBucket bucket = new TestBucket();
                        bucket.setName(testGroup.getVariable());
                        bucket.setValue(internalBucketId.getAndIncrement());
                        bucket.setDescription("`" + testGroup.getName() + "` " + testGroup.getDescription());
                        buckets.add(bucket);
                    });
            testDefinition.setBuckets(buckets);

            List<Allocation> allocations = Lists.newArrayList();
            Allocation allocation = new Allocation();
            List<Range> ranges = Lists.newArrayList();
            for (int i = 0; i < MAX_ALLOCATION; i++) {
                ranges.add(new Range(-1, 1.0 / MAX_ALLOCATION));
            }
            allocation.setRanges(ranges);
            allocations.add(allocation);
            testDefinition.setAllocations(allocations);

            allocateTraffic(testDefinition, test);

            testDefinitionMap.put(test.getTestId(), testDefinition);
        });
        artifact.setTests(testDefinitionMap);

        return artifact;
    }

    @Nonnull
    @Override
    public String getSource() {
        /**
         * 调试用。返回数据源的详细信息。
         */
        return inputURL.toString();
    }

}
