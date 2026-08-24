package org.example;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutorImpl implements Main.TaskExecutor {

    private final int maxConcurrency;
    private int active=0;

    private final ExecutorService workers;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    private final ArrayDeque<ScheduledTask<?>> pending = new ArrayDeque<>();
    private final Set<Main.TaskGroup> runningGroups = new HashSet<>();

    private final Thread dispatcher;

    TaskExecutorImpl(int maxConcurrency){
        if(maxConcurrency <= 0){
            throw new IllegalArgumentException();
        }
        this.maxConcurrency = maxConcurrency;
        workers = Executors.newFixedThreadPool(maxConcurrency);

        dispatcher = new Thread(this::dispatchLoop);
        dispatcher.start();
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        if(task == null) {
            throw new IllegalArgumentException();
        }
        lock.lock();
        try{
            ScheduledTask<T> scheduledTask = new ScheduledTask<>(task.taskGroup(), task.taskAction());
            pending.addLast(scheduledTask);
            changed.signalAll();

            return scheduledTask.futureTask;
        } finally {
            lock.unlock();
        }
    }

    private void dispatchLoop(){
        while(true){
            ScheduledTask<?> taskToRun;

            lock.lock();
            try{
                while(!canExecuteNextTask()){
                    changed.await();
                }
                taskToRun = pending.removeFirst();
                active++;
                runningGroups.add(taskToRun.taskGroup);
            } catch(InterruptedException e ) {
                Thread.currentThread().interrupt();
                return;
            }
            finally {
                lock.unlock();
            }

            workers.execute(()->runTask(taskToRun));
        }
    }

    private void runTask(ScheduledTask<?> taskToRun) {
        try{
            taskToRun.futureTask.run();
        } finally {
            lock.lock();
            try {
                active--;
                runningGroups.remove(taskToRun.taskGroup);
                changed.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    private boolean canExecuteNextTask(){
        ScheduledTask<?> nextTask = pending.peekFirst();
        return nextTask != null && active<maxConcurrency && !runningGroups.contains(nextTask.taskGroup);
    }

    private record ScheduledTask<T> (
            Main.TaskGroup taskGroup,
            FutureTask<T> futureTask
    ) {
            public ScheduledTask(Main.TaskGroup taskGroup, Callable<T> taskAction) {
                this(taskGroup, new FutureTask<T>(taskAction));
            }
    }
}
