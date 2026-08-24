package org.example;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TaskExecutorImplTest {

    @Test
    void shouldExecuteTaskAndReturnFutureResult() throws Exception {

        TaskExecutorImpl executor = new TaskExecutorImpl(2);

        Main.TaskGroup group = new Main.TaskGroup(UUID.randomUUID());
        Main.Task<String> task = new Main.Task<>(UUID.randomUUID(), group, Main.TaskType.READ, () -> "success");
        Future<String> result = executor.submitTask(task);

        assertEquals("success", result.get());
    }


    @Test
    void tasksFromDifferentGroupsCanRunInParallel() throws Exception {

        TaskExecutorImpl executor = new TaskExecutorImpl(2);

        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);

        Main.TaskGroup group1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup group2 = new Main.TaskGroup(UUID.randomUUID());

        Future<String> task1 = executor.submitTask(
                new Main.Task<>(
                        UUID.randomUUID(),
                        group1,
                        Main.TaskType.READ,
                        () -> {
                            started.countDown();
                            release.await();
                            return "task1";
                        }
                )
        );

        Future<String> task2 = executor.submitTask(
                new Main.Task<>(
                        UUID.randomUUID(),
                        group2,
                        Main.TaskType.WRITE,
                        () -> {
                            started.countDown();
                            release.await();
                            return "task2";
                        }
                )
        );

        assertTrue(started.await(2, TimeUnit.SECONDS));

        release.countDown();

        assertEquals("task1", task1.get());
        assertEquals("task2", task2.get());
    }
}