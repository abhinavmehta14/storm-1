package storm.mesos;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.util.MesosCommon;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class SupervisorSuicideDetector extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(MesosSupervisor.class);

  private final int timeoutSecs;
  private long lastTime = System.currentTimeMillis();
  private final AtomicReference<Set<Integer>> supervisorViewOfAssignedPorts;

  public SupervisorSuicideDetector(Map conf, AtomicReference<Set<Integer>> supervisorViewOfAssignedPorts) {
    timeoutSecs = MesosCommon.getSuicideTimeout(conf);
    this.supervisorViewOfAssignedPorts = supervisorViewOfAssignedPorts;
  }

  @Override
  public void run() {
    try {
      while (true) {
        long now = System.currentTimeMillis();
        if (!supervisorViewOfAssignedPorts.get().isEmpty()) {
          lastTime = now;
        }
        if ((now - lastTime) > 1000L * timeoutSecs) {
          LOG.info("Supervisor has not had anything assigned for {} secs. Committing suicide...", timeoutSecs);
          Runtime.getRuntime().halt(0);
        }
        Utils.sleep(5000);
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage());
      Runtime.getRuntime().halt(2);
    }
  }
}
