package org.mifos.connector.tnm.flowcomponents;

/**
 * Correlation store for paybill workflow instance keys keyed by TNM transaction id.
 */
public interface PaybillStateStore {

    void putWorkflowInstance(String txnId, String workflowInstanceKey);

    String getWorkflowInstance(String txnId);

    void removeWorkflowInstance(String txnId);
}
