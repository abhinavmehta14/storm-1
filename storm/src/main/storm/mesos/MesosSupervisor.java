/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.mesos;

import clojure.lang.PersistentVector;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.util.MesosCommon;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

public class MesosSupervisor implements ISupervisor {

  private static final Logger LOG = LoggerFactory.getLogger(MesosSupervisor.class);
  // Store state on port assignments arriving from MesosNimbus as task-launching requests.
  private static final TaskAssignments TASK_ASSIGNMENTS = TaskAssignments.getInstance();
  private Map conf;
  private ExecutorDriver mesosExecutorDriver;
  private StormExecutor stormExecutor;
  // What is the storm-core supervisor's view of the assigned ports?
  private AtomicReference<Set<Integer>> supervisorViewOfAssignedPorts = new AtomicReference<Set<Integer>>(new HashSet<Integer>());

  public static void main(String[] args) {
    Map<String, Object> conf = ConfigUtils.readStormConfig();

    try {
      Supervisor supervisor = new Supervisor(conf, null, new MesosSupervisor());
      supervisor.launchDaemon();
    } catch (Exception e) {
      String msg = format("main: Exception: %s", e.getMessage());
      LOG.error(msg);
      e.printStackTrace();
    }
  }

  /**
   * These ports are what is currently assigned to this supervisor instance.
   *
   * The storm-core supervisor takes the assignment from ZK (created by the Nimbus) and then
   * calls MesosSupervisor.confirmAssigned() on each port to verify whether the MesosNimbus has
   * assigned that port to this supervisor.
   *
   * @param ports The worker ports currently assigned to this supervisor. If empty, then this allows
   * the MesosSupervisor to learn there are no workers still assigned to this supervisor, and thus
   * triggers the eventual suicide of the MesosSupervisor to release any previously used ports.
   *
   * That "suicide" path deserves some explanation. This is the standard mechanism for a
   * MesosSupervisor to die. The "ports" parameter being empty means that the Nimbus has removed
   * all of this topology's workers from this host. So this tells the MesosSupervisor that there
   * are no more workers running, and as long as this state is maintained for some time, the
   * MesosSupervisor has no need to continue running.
   *
   * Note that if the supervisor is just killed (e.g., by "kill -9" on the CLI) then the held
   * worker ports will be released automatically by Mesos.
   *
   * Note that the suicide of the MesosSupervisor isn't necessary to release its ports under
   * normal conditions -- instead the storm-core supervisor will normally call
   * MesosSupervisor.killedWorker() to allow the MesosSupervisor to release a worker port
   * back to Mesos.  However, there exists a race condition wherein the MesosSupervisor.launchTask()
   * call will have recorded an assigned port into TASK_ASSIGNMENTS, but the storm-core supervisor
   * never actually launched the worker because the Nimbus scheduled away the worker before the
   * storm-core supervisor saw the assignment in ZK. And so storm-core supervisor would never call
   * MesosSupervisor.killedWorker() to release the port.  Hence the need for this
   * MesosSupervisor.assigned() logic.  See this GitHub issue for further explanation:
   *   https://github.com/mesos/storm/issues/227#issuecomment-338819387
   */
  @Override
  public void assigned(Collection<Integer> ports) {
    if (ports == null) ports = Collections.emptySet();
    LOG.info("storm-core supervisor has these ports assigned to it: {}", ports);
    supervisorViewOfAssignedPorts.set(new HashSet<>(ports));
  }

  @Override
  public void prepare(Map conf, String localDir) {
    stormExecutor = new StormExecutor();
    mesosExecutorDriver = new MesosExecutorDriver(stormExecutor);
    mesosExecutorDriver.start();
    LOG.info("Waiting for executor to initialize...");
    this.conf = conf;
    try {
      stormExecutor.waitUntilRegistered();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Executor initialized...");
    Thread suicide = new SupervisorSuicideDetector(conf, supervisorViewOfAssignedPorts);
    suicide.setDaemon(true);
    suicide.start();
  }

  /**
   * Called by storm-core supervisor to determine if the port is assigned to this
   * supervisor, and thus whether a corresponding worker process should be
   * killed or started.
   */
  @Override
  public boolean confirmAssigned(int port) {
    boolean isAssigned = TASK_ASSIGNMENTS.confirmAssigned(port);
    LOG.debug("confirming assignment for port {} as {}", port, isAssigned);
    return isAssigned;
  }

  @Override
  public Object getMetadata() {
    /*
     * Convert obtained Set into a List for 2 reasons:
     *  (1) Ensure returned object is serializable as required by storm's serialization
     *      of the SupervisorInfo while heartbeating.
     *      Previous to this change we were returning a ConcurrentHashMap$KeySetView,
     *      which is not necessarily serializable:
     *         http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4756277
     *  (2) Ensure we properly instantiate the PersistentVector with all ports.
     *      If given a Set the PersistentVector.create() method will simply wrap the passed
     *      Set as a single element in the new vector. Whereas if you pass a java.util.List
     *      or clojure.lang.ISeq, then you get a true vector composed of the elements of the
     *      List or ISeq you passed.
     */
    List ports = new ArrayList(TASK_ASSIGNMENTS.getAssignedPorts());
    return PersistentVector.create(ports);
  }

  @Override
  public String getSupervisorId() {
    // TODO: keep supervisorId local to this class
    return stormExecutor.getSupervisorId();
  }

  @Override
  public String getAssignmentId() {
    return MesosCommon.hostFromAssignmentId(stormExecutor.getAssignmentId(), MesosCommon.getWorkerPrefixDelimiter(conf));
  }

  @Override
  public void killedWorker(int port) {
    LOG.info("killedWorker: executor {} removing port {} assignment and sending " +
        "TASK_FINISHED update to Mesos", stormExecutor.getExecutorId(), port);
    TaskID taskId = TASK_ASSIGNMENTS.deregister(port);
    if (taskId == null) {
      LOG.error("killedWorker: Executor {} failed to find TaskID for port {}, so not " +
          "issuing TaskStatus update to Mesos for this dead task", stormExecutor.getExecutorId(), port);
      return;
    }
    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_FINISHED)
        .setTaskId(taskId)
        .build();
    mesosExecutorDriver.sendStatusUpdate(status);
  }

}
