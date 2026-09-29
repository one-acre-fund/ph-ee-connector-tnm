package org.mifos.connector.tnm.zeebe;

import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_TRX_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSFER_CREATE_FAILED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.ZeebeFuture;
import io.camunda.zeebe.client.api.command.CompleteJobCommandStep1;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobWorkerBuilderStep1;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mifos.connector.tnm.flowcomponents.PaybillStateStore;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ZeebeWorkersTest {

    @Mock
    private ZeebeClient zeebeClient;

    @Mock
    private PaybillStateStore paybillStateStore;

    @Mock
    private JobWorkerBuilderStep1 workerBuilder;

    @Mock
    private JobWorkerBuilderStep1.JobWorkerBuilderStep2 workerBuilderStep2;

    @Mock
    private JobWorkerBuilderStep1.JobWorkerBuilderStep3 workerBuilderStep3;

    @Mock
    private JobClient jobClient;

    @Mock
    private ActivatedJob job;

    @Mock
    private CompleteJobCommandStep1 completeCommand;

    private JobHandler cleanupHandler;

    @BeforeEach
    void setUp() throws Exception {
        when(zeebeClient.newWorker()).thenReturn(workerBuilder);
        when(workerBuilder.jobType("delete-tnm-workflow-instancekey")).thenReturn(workerBuilderStep2);
        when(workerBuilderStep2.handler(any())).thenReturn(workerBuilderStep3);
        when(workerBuilderStep3.name(anyString())).thenReturn(workerBuilderStep3);
        when(workerBuilderStep3.maxJobsActive(anyInt())).thenReturn(workerBuilderStep3);

        ZeebeWorkers zeebeWorkers = new ZeebeWorkers(zeebeClient, paybillStateStore);
        ReflectionTestUtils.setField(zeebeWorkers, "workerMaxJobs", 5);
        zeebeWorkers.setupWorkers();

        ArgumentCaptor<JobHandler> handlerCaptor = ArgumentCaptor.forClass(JobHandler.class);
        verify(workerBuilderStep2).handler(handlerCaptor.capture());
        cleanupHandler = handlerCaptor.getValue();

        when(jobClient.newCompleteCommand(any(Long.class))).thenReturn(completeCommand);
        when(completeCommand.variables(anyMap())).thenReturn(completeCommand);
        when(completeCommand.send()).thenReturn(mock(ZeebeFuture.class));
        when(job.getKey()).thenReturn(42L);
    }

    @Test
    @DisplayName("Cleanup worker removes workflow instance when tnmTrxId is present")
    void cleanup_shouldRemoveWorkflowInstanceWhenTxnIdPresent() throws Exception {
        Map<String, Object> variables = new HashMap<>();
        variables.put(TNM_TRX_ID, "tnm-txn-1");
        when(job.getVariablesAsMap()).thenReturn(variables);

        cleanupHandler.handle(jobClient, job);

        verify(paybillStateStore).removeWorkflowInstance("tnm-txn-1");
        verify(completeCommand).variables(eq(Map.of(TRANSFER_CREATE_FAILED, true)));
        verify(completeCommand).send();
    }

    @Test
    @DisplayName("Cleanup worker skips Redis remove when tnmTrxId is missing")
    void cleanup_shouldSkipRemoveWhenTxnIdMissing() throws Exception {
        Map<String, Object> variables = new HashMap<>();
        variables.put("otherKey", "value");
        when(job.getVariablesAsMap()).thenReturn(variables);

        cleanupHandler.handle(jobClient, job);

        verify(paybillStateStore, never()).removeWorkflowInstance(anyString());
        verify(completeCommand).variables(eq(Map.of(TRANSFER_CREATE_FAILED, true)));
    }

    @Test
    @DisplayName("Cleanup worker skips Redis remove when variables map is empty")
    void cleanup_shouldSkipRemoveWhenVariablesEmpty() throws Exception {
        when(job.getVariablesAsMap()).thenReturn(Collections.emptyMap());

        cleanupHandler.handle(jobClient, job);

        verify(paybillStateStore, never()).removeWorkflowInstance(anyString());
        verify(completeCommand).variables(eq(Map.of(TRANSFER_CREATE_FAILED, true)));
    }

    @Test
    @DisplayName("Cleanup worker skips remove when tnmTrxId value is null")
    void cleanup_shouldSkipRemoveWhenTxnIdValueNull() throws Exception {
        Map<String, Object> variables = new HashMap<>();
        variables.put(TNM_TRX_ID, null);
        when(job.getVariablesAsMap()).thenReturn(variables);

        cleanupHandler.handle(jobClient, job);

        verify(paybillStateStore, never()).removeWorkflowInstance(anyString());
        verify(completeCommand).send();
    }

    @Test
    @DisplayName("Cleanup worker converts non-string tnmTrxId via toString")
    void cleanup_shouldConvertTxnIdWithToString() throws Exception {
        Map<String, Object> variables = new HashMap<>();
        variables.put(TNM_TRX_ID, 12345);
        when(job.getVariablesAsMap()).thenReturn(variables);

        cleanupHandler.handle(jobClient, job);

        verify(paybillStateStore).removeWorkflowInstance("12345");
    }
}
