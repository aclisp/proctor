package com.indeed.proctor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.proctor.common.admin.model.Test;
import com.indeed.proctor.common.admin.model.TestGroup;
import com.indeed.proctor.common.admin.model.TestMatrix;
import com.indeed.proctor.common.model.*;
import com.indeed.proctor.common.server.Config;
import com.indeed.proctor.common.server.InputContexts;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;


/**
 * 从远程下载指定格式的实验描述JSON
 */
public class RemoteProctorLoader extends AbstractProctorLoader {
    private static final Logger LOGGER = Logger.getLogger(RemoteProctorLoader.class);

    private static final int MAX_ALLOCATION = 10000;

    @Nonnull
    public URL getInputURL() {
        return inputURL;
    }

    @Nonnull
    private final URL inputURL = new URL(Config.EXPERIMENT_SOURCE);
    //private final URL inputURL = new URL("https://abtest.yy.com/api/testList");
    @Nonnull
    private final ObjectMapper objectMapper = Serializers.lenient();
    @Nullable
    private TestMatrixArtifact cachedArtifact = null;

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
        /*
         * 这里根据数据源构造出TestMatrixArtifact，给AbstractProctorLoader做进一步验证。
         */
        try (final Reader reader = new BufferedReader(new InputStreamReader(inputURL.openStream(), StandardCharsets.UTF_8))) {
            return loadJsonTestMatrix(reader);
        }
    }

    @Nullable
    protected TestMatrixArtifact loadJsonTestMatrix(@Nonnull final Reader reader) throws IOException {
        try {
            List<Test> tests = Arrays.asList(objectMapper.readValue(reader, Test[].class));
            final TestMatrix testMatrix = new TestMatrix();
            testMatrix.setTests(tests);
            return createArtifact(testMatrix);
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
                        LOGGER.info("bucket `" + test.getTestId() + "/" + bucket.getName() + "/" + bucket.getValue() +
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
                    LOGGER.info("bucket `" + test.getTestId() + "/" + bucket.getName() + "/" + bucket.getValue() +
                            "` has a ratio of " + count + "/" + total + " after allocation");
                });
    }

    @Nonnull
    private TestGroup findTestGroupByVariable(Test test, String variable) {
        return test.getTestGroups().stream().filter(testGroup -> testGroup.getVariable().equals(variable))
                .findFirst().get();
    }

    @Nonnull
    private String getTestMatrixVersion(@Nonnull final TestMatrix testMatrix) {
        // Version以所有实验的内容的md5为准
        MessageDigest testsDigest = ProctorUtils.createMessageDigest();
        testMatrix.getTests().stream()
                .filter(eligibleTest())
                .sorted(Comparator.comparing(Test::getTestId))
                .forEachOrdered(test -> {
                    try {
                        testsDigest.update(objectMapper.writeValueAsBytes(test));
                    } catch (JsonProcessingException e) {
                        LOGGER.error("exception got during digest test `" + test.getTestId() + "`", e);
                    }
                });
        return Base64.getEncoder().encodeToString(testsDigest.digest());
    }

    private static Predicate<Test> eligibleTest() {
        return test -> {
            switch (test.getState()) {
                case Test.STATE_RUNNING:
                    return true;
                case Test.STATE_PAUSED:
                    return true;
                default:
                    return false;
            }
        };
    }

    @Nonnull
    private TestMatrixArtifact createArtifact(@Nonnull final TestMatrix testMatrix) {
        String testMatrixVersion = getTestMatrixVersion(testMatrix);
        if (cachedArtifact != null && cachedArtifact.getAudit().getVersion().equals(testMatrixVersion)) {
            return cachedArtifact;
        }

        TestMatrixArtifact artifact = new TestMatrixArtifact();

        Audit audit = new Audit();
        audit.setVersion(testMatrixVersion);
        audit.setUpdated(System.currentTimeMillis());
        audit.setUpdatedBy(RemoteProctorLoader.class.getName());
        artifact.setAudit(audit);

        Map<String, ConsumableTestDefinition> testDefinitionMap = Maps.newHashMap();
        testMatrix.getTests().stream().filter(eligibleTest()).forEach(test -> {
            ConsumableTestDefinition testDefinition = new ConsumableTestDefinition();
            testDefinition.setSalt(test.getTestId());
            testDefinition.setVersion(test.getTestId() + "." + String.valueOf(test.getId()));
            testDefinition.setDescription("`" + test.getName() + "` " + test.getDescription());
            switch (test.getType()) {
                case Test.TYPE_DEVICEID:
                    testDefinition.setTestType(TestType.DEVICE_ID);
                    break;
                case Test.TYPE_UID:
                    testDefinition.setTestType(TestType.USER_ID);
                    break;
                default:
                    testDefinition.setTestType(TestType.RANDOM);
            }
            // 设置测试规则
            List<String> targetRules = Lists.newArrayList();
            test.getTargets().forEach(target -> {
                switch (target) {
                    case Test.TARGET_NEW:
                        targetRules.add(InputContexts.isBrandNewUser);
                        break;
                    case Test.TARGET_ALL:
                        targetRules.add("true");
                        break;
                    case "uidEndsWith1":
                        // intentionally do not use fn:endsWith
                        // https://stackoverflow.com/questions/16750540/jstl-bug-in-function-endswith
                        targetRules.add("proctor:endsWith(userId, '1')");
                        break;
                    case "hdid_last_digit_one_two":
                        targetRules.add("proctor:endsWith(proctor:hdidToDigit(deviceId), '1')");
                        targetRules.add("proctor:endsWith(proctor:hdidToDigit(deviceId), '2')");
                        break;
                    default:
                        LOGGER.warn("unknown test target `" + target + "`: no rule can be added");
                }
            });
            // 根据测试状态改写规则
            switch (test.getState()) {
                case Test.STATE_RUNNING:
                    break;
                case Test.STATE_PAUSED:
                    // 暂停实验必须不能参与计算，这样新用户看不到它
                    targetRules.clear();
                    targetRules.add("false");
                    break;
                default:
                    targetRules.clear();
                    targetRules.add("false");
            }
            testDefinition.setState(Strings.nullToEmpty(test.getState()));
            testDefinition.setRule("${" + String.join("||", targetRules) + "}");

            List<TestBucket> buckets = Lists.newArrayList();
            buckets.add(new TestBucket("inactive", -1, ""));
            Map<String, TestBucket> whiteList = Maps.newHashMap();
            // 按TestGroup的variable排序
            AtomicInteger internalBucketId = new AtomicInteger(0);
            test.getTestGroups().stream().sorted(Comparator.comparing(TestGroup::getVariable))
                    .forEachOrdered(testGroup -> {
                        TestBucket bucket = new TestBucket();
                        bucket.setName(testGroup.getVariable());
                        bucket.setValue(internalBucketId.getAndIncrement());
                        bucket.setDescription("`" + testGroup.getName() + "` " + testGroup.getDescription());
                        buckets.add(bucket);
                        testGroup.getWhiteList().forEach(identifier -> whiteList.put(identifier, bucket));
                    });
            testDefinition.setBuckets(buckets);
            testDefinition.setWhiteList(whiteList);

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

        cachedArtifact = artifact;
        return artifact;
    }

    @Nonnull
    @Override
    public String getSource() {
        /*
         * 调试用。返回数据源的详细信息。
         */
        return inputURL.toString();
    }

}
