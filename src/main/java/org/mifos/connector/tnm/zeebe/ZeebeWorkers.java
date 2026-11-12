package org.mifos.connector.tnm.zeebe;

import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_TRX_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSFER_CREATE_FAILED;

import io.camunda.zeebe.client.ZeebeClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.tnm.flowcomponents.PaybillStateStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Contains workers that will run based on the BPMN flow.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ZeebeWorkers {

    private final ZeebeClient zeebeClient;
    private final PaybillStateStore paybillStateStore;
    private final ZeebeWorkerDrain drain;

    @Value("${zeebe.client.evenly-allocated-max-jobs}")
    private int workerMaxJobs;

    // Workers open only once the application is ready, after Camel and the web server have
    // started. Opened earlier, a starting pod activates jobs it cannot process yet, and a pod
    // replaced during startup would hold them until the broker job timeout (FD-1939).
    @EventListener(ApplicationReadyEvent.class)
    void setupWorkers() {

        drain.register(zeebeClient.newWorker().jobType("delete-tnm-workflow-instancekey").handler(drain.track(((client, job) -> {
            Map<String, Object> variables = job.getVariablesAsMap();
            if (!CollectionUtils.isEmpty(variables)) {
                Object tnmTxnIdObj = variables.get(TNM_TRX_ID);
                if (Objects.nonNull(tnmTxnIdObj)) {
                    String tnmTxnId = tnmTxnIdObj.toString();
                    log.info("Txn Id Removed :{}", tnmTxnId);
                    paybillStateStore.removeWorkflowInstance(tnmTxnId);
                }

            }
            Map<String, Object> completionVariables = new HashMap<>();
            completionVariables.put(TRANSFER_CREATE_FAILED, true);
            client.newCompleteCommand(job.getKey()).variables(completionVariables).send().join();
        }))).name("Cleanup").maxJobsActive(workerMaxJobs).open());
    }

}
