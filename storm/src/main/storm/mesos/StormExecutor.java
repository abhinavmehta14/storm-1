package storm.mesos;


import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.util.MesosCommon;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;

class StormExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(StormExecutor.class);

  private static final TaskAssignments TASK_ASSIGNMENTS = TaskAssignments.getInstance();
  private final CountDownLatch registeredLatch = new CountDownLatch(1);
  private String assignmentId;
  private String executorId;
  private String supervisorId;

  public String getExecutorId() {
    return executorId;
  }

  public String getSupervisorId() {
    return supervisorId;
  }

  public String getAssignmentId() {
    return assignmentId;
  }

  public void waitUntilRegistered() throws InterruptedException {
    registeredLatch.await();
  }

  @Override
  public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
    LOG.info("Received executor data <{}>", executorInfo.getData().toStringUtf8());
    Map ids = (Map) JSONValue.parse(executorInfo.getData().toStringUtf8());
    executorId = executorInfo.getExecutorId().getValue();
    supervisorId = (String) ids.get(MesosCommon.SUPERVISOR_ID);
    assignmentId = (String) ids.get(MesosCommon.ASSIGNMENT_ID);
    LOG.info("Registered supervisor with Mesos: {}, {}", supervisorId, assignmentId);

    // Completed registration, let anything waiting for us to do so continue
    registeredLatch.countDown();
  }


  @Override
  public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {
    try {
      int port = TASK_ASSIGNMENTS.register(task.getTaskId());
      LOG.info("Executor {} received task assignment for port {}. Mesos TaskID: {}",
          executorId, port, task.getTaskId().getValue());
    } catch (IllegalArgumentException e) {
      String msg = format("launchTask: failed to register task. " +
            "Exception: %s Halting supervisor process",
          e.getMessage());
      LOG.error(msg);
      Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
          .setState(Protos.TaskState.TASK_FAILED)
          .setTaskId(task.getTaskId())
          .setMessage(msg)
          .build();
      driver.sendStatusUpdate(status);
      Runtime.getRuntime().halt(1);
    }
    LOG.info("Received task assignment for TaskID: {}",
        task.getTaskId().getValue());
    Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
        .setState(Protos.TaskState.TASK_RUNNING)
        .setTaskId(task.getTaskId())
        .build();
    driver.sendStatusUpdate(status);
  }

  @Override
  public void killTask(ExecutorDriver driver, Protos.TaskID id) {
    LOG.warn("killTask not implemented in executor {}, so " +
        "cowardly refusing to kill task {}", executorId, id.getValue());
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.warn("shutdown not implemented in executor {}, so " +
        "cowardly refusing to kill tasks", executorId);
  }

  @Override
  public void error(ExecutorDriver driver, String msg) {
    LOG.error("Received fatal error \nmsg: {} \nHalting process...", msg);
    Runtime.getRuntime().halt(2);
  }

  @Override
  public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
    LOG.info("executor has reregistered with the mesos-slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    LOG.info("executor has disconnected from the mesos-slave");
  }
}
