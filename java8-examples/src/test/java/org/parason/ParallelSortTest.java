package org.parason;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * There's a problem with Javas Fork-Join Framework: an Exception (in this particular test an NPE in the comparator) during
 * the parallel processing of a common task will result a deadlock. Not using multi-threading or configuring
 * -Djava.util.concurrent.ForkJoinPool.common.parallelism=1 JVM property will of course solve the issue.
 * <p>
 * This was tested on the following java versions:
 * <p>
 * java version "1.8.0_40"
 * Java(TM) SE Runtime Environment (build 1.8.0_40-b25)
 * Java HotSpot(TM) 64-Bit Server VM (build 25.40-b25, mixed mode)
 * <p>
 * java version "1.8.0_51"
 * Java(TM) SE Runtime Environment (build 1.8.0_51-b16)
 * Java HotSpot(TM) 64-Bit Server VM (build 25.51-b03, mixed mode)
 *
 * @author parason
 */
public class ParallelSortTest {

    private static final int MIN_ARRAY_SORT_GRAN = 1 << 13; // as in java.util.Arrays#MIN_ARRAY_SORT_GRAN
    private static final int LIST_SIZE           = MIN_ARRAY_SORT_GRAN + 1; // should be enough to demonstrate the issue

    private List<TestEntity> underTest; // test subject

    /**
     * Generate some random stuff for tests
     */
    @Before
    public void before() {
        underTest = Stream.generate(() -> new TestEntity(ThreadLocalRandom.current().nextLong(),
                                                         UUID.randomUUID().toString()))
                .limit(LIST_SIZE)
                .collect(Collectors.toList());
    }

    /**
     * Setting the ID of a randomly chosen entity from the right side of the test list and try to sort it with
     * {@link Arrays#parallelSort(Object[], Comparator)}
     * This test currently fails with the timeout
     */
    @Test(timeout = 2_000L)
    public void testArraysParallelSort_RightSide() {
        TestEntity testEntity = underTest.get(ThreadLocalRandom.current().nextInt(LIST_SIZE / 2, LIST_SIZE - 1));
        testEntity.setId(null);

        TestEntity[] testEntityArray = {};

        try {
            Arrays.parallelSort(underTest.toArray(testEntityArray), NO_NULL_CHECK);
            Assert.fail("Expected an exception that has never been thrown");
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
    }

    /**
     * Setting the ID of a randomly chosen entity from the left side of the test list and try to sort it with
     * {@link Arrays#parallelSort(Object[], Comparator)}
     * This test currently fails with the timeout
     */
    @Test(timeout = 2_000L)
    public void testArraysParallelSort_LeftSide() {
        TestEntity testEntity = underTest.get(ThreadLocalRandom.current().nextInt(0, LIST_SIZE / 2 - 1));
        testEntity.setId(null);

        TestEntity[] testEntityArray = {};

        try {
            Arrays.parallelSort(underTest.toArray(testEntityArray), NO_NULL_CHECK);
            Assert.fail("Expected an exception that has never been thrown");
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
    }

    /**
     * Setting the ID of a randomly chosen entity from the left side of the test list and try to sort it using {@link Stream} API
     * This test currently passes, but only if the randomly selected index is located near the beginning of the list
     */
    @Test(timeout = 2_000L)
    public void testStreamParallelSort_LeftSide() {
        TestEntity testEntity = underTest.get(ThreadLocalRandom.current().nextInt(0, LIST_SIZE / 2 - 1));

        testEntity.setId(null);

        try {
            underTest = underTest.parallelStream().sorted(NO_NULL_CHECK).collect(Collectors.toList());
            Assert.fail("Expected an exception that has never been thrown");
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
    }

    /**
     * Setting the ID of a randomly chosen entity from the right side of the test list and try to sort it using {@link Stream} API
     * This test currently fails with the timeout
     */
    @Test(timeout = 2_000L)
    public void testStreamParallelSort_RightSide() {
        TestEntity testEntity = underTest.get(ThreadLocalRandom.current().nextInt(LIST_SIZE / 2, LIST_SIZE - 1));
        testEntity.setId(null);

        TestEntity[] testEntityArray = {};

        try {
            Arrays.parallelSort(underTest.toArray(testEntityArray), NO_NULL_CHECK);
            Assert.fail("Expected an exception that has never been thrown");
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
    }

    /**
     * The {@link Comparator} without a null check (actually can be any Exception)
     */
    private static final Comparator<TestEntity> NO_NULL_CHECK = (a, b) -> {
        if (a.id.equals(b.id)) {
            return 0;
        }

        if (a.id > b.id) {
            return 1;
        }

        if (a.id < b.id) {
            return -1;
        }

        throw new IllegalStateException("Uncovered comparison");
    };

    /**
     * A simple entity with an ID and description
     */
    private static class TestEntity {
        private Long   id;
        private String description;

        private TestEntity(Long id, String description) {
            this.id = id;
            this.description = description;
        }

        public Long getId() {
            return id;
        }

        public String getDescription() {
            return description;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestEntity that = (TestEntity) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(description, that.description);
        }

        @Override
        public int hashCode() {

            return Objects.hash(id, description);
        }
    }

}
