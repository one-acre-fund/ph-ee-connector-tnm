package org.mifos.connector.tnm.camel.config;

/**
 * Contains properties related to camel.
 */
public class CamelProperties {

    private CamelProperties() {}

    public static final String CHANNEL_REQUEST = "channelRequest";

    // HTTP Headers
    public static final String CONTENT_TYPE_VAL = "application/json";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CAMEL_HTTP_RESPONSE_CODE = "CamelHttpResponseCode";
    public static final String CHANNEL_URL_PROPERTY_IN_HEADER = "${header.channelUrl}";
    public static final String BRIDGE_ENDPOINT_QUERY_PARAM = "?bridgeEndpoint=true&throwExceptionOnFailure=false";

    public static final String BUSINESS_SHORT_CODE = "BusinessShortCode";
    public static final String FINERACT_PRIMARY_IDENTIFIER_NAME = "fineractAccountID";
    public static final String ROSTER_PRIMARY_IDENTIFIER_NAME = "ACCOUNTID";
    public static final String SECONDARY_IDENTIFIER_NAME = "MSISDN";
    public static final String GET_ACCOUNT_DETAILS_FLAG = "getAccountDetails";
    public static final String CUSTOM_HEADER_FILTER_STRATEGY = "customHeaderFilterStrategy";
    public static final String X_CORRELATION_ID = "X-CorrelationID";
    public static final String ACCOUNT_HOLDING_INSTITUTION_ID = "accountHoldingInstitutionId";
    public static final String AMS_NAME = "amsName";
    public static final String TENANT_ID = "Platform-TenantId";
    public static final String CLIENT_ACCOUNT_NUMBER = "clientAccountNumber";
    public static final String CHANNEL_URL = "channelUrl";
    public static final String CLIENT_NAME = "clientName";
    public static final String PAYBILL_TRANSACTION_ID_URL_PARAM = "paybillTransactionId";
    public static final String TNM_PAYBILL_WORKFLOW_SUBTYPE = "inbound";
    public static final String TNM_PAYBILL_WORKFLOW_TYPE = "tnm";

    public static final String TNM_TRX_ID = "tnmTrxId";
    public static final String TNM_PAY_REQUEST_PAY_WAIT_PERIOD = "tnmPayRequestWaitPeriod";
    public static final String TNM_PAY_OAF_TRANSACTION_REFERENCE = "oafTransactionReference";
    public static final String TNM_VALIDATION_REQUEST_PAYLOAD = "validationRequest";
    public static final String PAYBILL_TRANSFER_CODE = "transferCode";
    public static final String PAYMENT_SCHEME = "paymentScheme";
    public static final String TNM_CONSTANT = "tnm";
}
