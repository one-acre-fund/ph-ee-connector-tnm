package org.mifos.connector.tnm.camel.routes;

import static org.mifos.connector.tnm.camel.config.CamelProperties.ACCOUNT_HOLDING_INSTITUTION_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.AMS_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.BUSINESS_SHORT_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CHANNEL_REQUEST;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CHANNEL_URL;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_ACCOUNT_NUMBER;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CONTENT_TYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CONTENT_TYPE_VAL;
import static org.mifos.connector.tnm.camel.config.CamelProperties.GET_ACCOUNT_DETAILS_FLAG;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYBILL_TRANSACTION_ID_URL_PARAM;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYBILL_TRANSFER_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYMENT_SCHEME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.SECONDARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_CONSTANT;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_PAYBILL_WORKFLOW_SUBTYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_PAYBILL_WORKFLOW_TYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_PAY_OAF_TRANSACTION_REFERENCE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_TRX_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.X_CORRELATION_ID;
import static org.mifos.connector.tnm.util.TnmUtils.buildPayBillValidationResponse;
import static org.mifos.connector.tnm.util.TnmUtils.generateWorkflowId;
import static org.mifos.connector.tnm.util.TnmUtils.getPrimaryIdentifierName;
import static org.mifos.connector.tnm.util.TnmUtils.getTnmPayRequestPayWaitPeriod;
import static org.mifos.connector.tnm.util.TnmUtils.getWorkflowId;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.CURRENCY;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.EXTERNAL_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.IS_VALIDATION_REFERENCE_PRESENT;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.SERVER_TRANSACTION_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSFER_CREATE_FAILED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.ZeebeClient;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.json.JSONObject;
import org.mifos.connector.common.channel.dto.TransactionStatusResponseDTO;
import org.mifos.connector.common.gsma.dto.GsmaTransfer;
import org.mifos.connector.common.mojaloop.type.TransferState;
import org.mifos.connector.tnm.camel.config.AmsPayBillProperties;
import org.mifos.connector.tnm.camel.config.AmsProperties;
import org.mifos.connector.tnm.camel.config.CamelProperties;
import org.mifos.connector.tnm.camel.config.ZeebeProperties;
import org.mifos.connector.tnm.dto.ChannelRequestDto;
import org.mifos.connector.tnm.dto.ChannelValidationRequestDto;
import org.mifos.connector.tnm.dto.PayBillValidationResponseDto;
import org.mifos.connector.tnm.dto.TnmPayBillPayRequestDto;
import org.mifos.connector.tnm.exception.MissingFieldException;
import org.mifos.connector.tnm.exception.TnmConnectorExistingTransactionIdException;
import org.mifos.connector.tnm.exception.TnmConnectorJsonProcessingException;
import org.mifos.connector.tnm.util.TnmUtils;
import org.mifos.connector.tnm.zeebe.ZeebeVariables;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Class to process the PayBill route.
 *
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PayBillRouteProcessor {

    private final ProducerTemplate producerTemplate;
    private final ZeebeClient zeebeClient;
    private final AmsPayBillProperties amsPayBillProps;

    private final ZeebeProperties zeebeProperties;

    @Value("${channel.host}")
    private String channelUrl;

    private ObjectMapper objectMapper = TnmUtils.getObjectMapper();
    public static Map<String, Boolean> reconciledStore = new HashMap<>();
    public static Map<String, String> workflowInstanceStore = new HashMap<>();

    @Value("${tenant}")
    private String tenantId;

    /**
     * Build the request body for get account status request.
     *
     * @param exchange
     *            {@link Exchange}
     * @return a stringified request body.
     */
    public String buildBodyForAccountStatus(Exchange exchange) {
        log.debug("## PayBill Validation Payload request");
        final String clientAccountNumber = exchange.getIn().getHeader(CLIENT_ACCOUNT_NUMBER).toString();
        final Object currencyFromHeaders = exchange.getIn().getHeader(CURRENCY);
        final Object shortCodeFromReq = exchange.getIn().getHeader(BUSINESS_SHORT_CODE);
        final Object msisdn = exchange.getIn().getHeader(SECONDARY_IDENTIFIER_NAME);
        final Object getAccountDetails = exchange.getIn().getHeader(GET_ACCOUNT_DETAILS_FLAG);
        final boolean getAccountDetailsFlag = !Objects.nonNull(getAccountDetails) || Boolean.parseBoolean(getAccountDetails.toString());

        if (Objects.isNull(msisdn)) {
            throw new MissingFieldException("MSISDN is required for PayBill validation");
        }

        AmsProperties amsProperties = amsPayBillProps.getAmsPropertiesFromShortCode(
                Objects.nonNull(shortCodeFromReq) ? shortCodeFromReq.toString() : amsPayBillProps.getDefaultAmsShortCode());
        final String amsName = amsProperties.getAms();
        final String currency = Objects.nonNull(currencyFromHeaders) ? currencyFromHeaders.toString() : amsProperties.getCurrency();
        exchange.getIn().removeHeaders("*");
        exchange.getIn().setHeader("amsUrl", amsProperties.getBaseUrl());
        exchange.getIn().setHeader(CONTENT_TYPE, CONTENT_TYPE_VAL);
        exchange.getIn().setHeader("amsName", amsName);
        exchange.getIn().setHeader("accountHoldingInstitutionId", amsPayBillProps.getAccountHoldingInstitutionId());
        exchange.setProperty("channelUrl", channelUrl);
        exchange.setProperty("primaryIdentifier", getPrimaryIdentifierName(amsName));
        exchange.setProperty("primaryIdentifierValue", clientAccountNumber);
        exchange.setProperty("secondaryIdentifier", SECONDARY_IDENTIFIER_NAME);
        exchange.setProperty("secondaryIdentifierValue", msisdn);
        ChannelValidationRequestDto obj = TnmUtils.createValidationRequest(clientAccountNumber, amsName, currency, msisdn.toString(),
                getAccountDetailsFlag);
        log.debug("Header:{}", exchange.getIn().getHeaders());
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException ex) {
            throw new TnmConnectorJsonProcessingException(ex.getMessage(), ex);
        }
    }

    /**
     * Build the request body for start PayBill workflow.
     *
     * @param e
     *            {@link Exchange}
     * @return a stringified request body.
     */
    public String buildBodyForStartPayBillWorkflow(Exchange e) {

        PayBillValidationResponseDto validationResponseDto = e.getIn().getBody(PayBillValidationResponseDto.class);
        boolean isReconciled = validationResponseDto.isReconciled();
        String validationTransactionId = validationResponseDto.getTransactionId();
        log.debug("PayBill Response: {}", validationResponseDto);
        log.debug("Validation IsReconciled: {}", isReconciled);
        log.debug("Validation Transaction ID present: {}", validationTransactionId);

        // Add the validation transactionID as clientCorrelationId in the reconciledStore
        String clientCorrelationId = validationTransactionId;
        reconciledStore.put(clientCorrelationId, isReconciled);

        try {

            e.getIn().removeHeaders("*");
            e.getIn().setHeader(ACCOUNT_HOLDING_INSTITUTION_ID, validationResponseDto.getAccountHoldingInstitutionId());
            e.getIn().setHeader(AMS_NAME, validationResponseDto.getAmsName());
            e.getIn().setHeader(TENANT_ID, tenantId);
            e.getIn().setHeader(X_CORRELATION_ID, clientCorrelationId);
            e.getIn().setHeader(CONTENT_TYPE, CONTENT_TYPE_VAL);
            e.getIn().setHeader(CLIENT_NAME, validationResponseDto.getClientName());

            e.setProperty("isValidationReferencePresent", isReconciled);
            e.setProperty("channelUrl", channelUrl);

            GsmaTransfer gsmaTransfer = TnmUtils.createGsmaTransferDto(validationResponseDto, clientCorrelationId,
                    zeebeProperties.getWaitTnmPayRequestPeriod());

            return objectMapper.writeValueAsString(gsmaTransfer);
        } catch (JsonProcessingException ex) {
            throw new TnmConnectorJsonProcessingException(ex.getMessage(), ex);
        }
    }

    /**
     * Process the request for PayBill pay route.
     *
     * @param e
     *            {@link Exchange}
     */
    public void processRequestForPayBillPayRoute(Exchange e) {
        TnmPayBillPayRequestDto requestDto = e.getIn().getBody(TnmPayBillPayRequestDto.class);
        log.debug("PayBill Response: {}", requestDto);
        log.debug("Is oafTransactionReference present: {}", requestDto.getOafValidationRef());
        final Object currencyFromHeaders = e.getIn().getHeader(CURRENCY);
        final Object shortCodeFromReq = e.getIn().getHeader(BUSINESS_SHORT_CODE);

        AmsProperties amsProperties = amsPayBillProps.getAmsPropertiesFromShortCode(
                Objects.nonNull(shortCodeFromReq) ? shortCodeFromReq.toString() : amsPayBillProps.getDefaultAmsShortCode());
        final String amsName = amsProperties.getAms();
        final String currency = Objects.nonNull(currencyFromHeaders) ? currencyFromHeaders.toString() : amsProperties.getCurrency();
        final String amsUrl = amsProperties.getBaseUrl();

        Boolean isReconciled = Objects.nonNull(requestDto.getOafValidationRef());
        requestDto.setValidationReferencePresent(isReconciled);
        requestDto.setAmsName(amsProperties.getAms());

        e.setProperty("amsUrl", amsUrl);
        e.setProperty("secondaryIdentifier", "MSISDN");
        e.setProperty("secondaryIdentifierValue", requestDto.getMsisdn());
        try {

            validateUniqueTransactionId(requestDto.getTransactionId());
        } catch (JsonProcessingException ex) {
            throw new TnmConnectorJsonProcessingException(ex.getMessage(), ex);
        }

        ChannelRequestDto channelRequestDto = TnmUtils.convertPayBillToChannelPayload(requestDto, amsName, currency);
        channelRequestDto.setUseWorkflowIdAsTransactionId(true);

        String oafTransactionReference = requestDto.getOafValidationRef();
        channelRequestDto.setWorkflowId(oafTransactionReference);

        Map<String, Object> variables = new HashMap<>();
        variables.put("confirmationReceived", true);
        variables.put(CHANNEL_REQUEST, channelRequestDto.toString());
        variables.put("amount", requestDto.getTransactionAmount());
        variables.put("accountId", requestDto.getAccountNumber());
        variables.put("originDate", Long.parseLong(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))));
        variables.put("phoneNumber", requestDto.getMsisdn());
        variables.put(PAYMENT_SCHEME, TNM_CONSTANT);
        String tnmTransactionId = requestDto.getTransactionId();
        variables.put(SERVER_TRANSACTION_ID, tnmTransactionId);
        variables.put(EXTERNAL_ID, tnmTransactionId);

        // Getting TNM workflow transaction id and removing key
        String workflowInstanceKey = workflowInstanceStore.get(oafTransactionReference);

        variables.put(IS_VALIDATION_REFERENCE_PRESENT, isReconciled && workflowInstanceKey != null);
        variables.put(TRANSACTION_ID, tnmTransactionId);
        variables.put("clientCorrelationId", oafTransactionReference);
        variables.put(TRANSFER_CREATE_FAILED, false);
        log.info("Workflow transaction id : {}", workflowInstanceKey);

        if (workflowInstanceKey != null) {
            zeebeClient.newPublishMessageCommand().messageName("pendingPayRequest").correlationKey(oafTransactionReference)
                    .timeToLive(Duration.ofMillis(300)).variables(variables).send();
            log.debug("Published Variables");
        } else {
            log.debug("No workflow of such transaction ID exists");
            workflowInstanceKey = generateWorkflowId();
            oafTransactionReference = workflowInstanceKey;
            variables.put("clientCorrelationId", workflowInstanceKey);
            variables.putIfAbsent(TNM_TRX_ID, oafTransactionReference);
            variables.putIfAbsent(PAYBILL_TRANSFER_CODE, oafTransactionReference);
            variables.put(ZeebeVariables.ORIGIN_DATE, Instant.now().toEpochMilli());
            variables.put(CamelProperties.TNM_PAY_REQUEST_PAY_WAIT_PERIOD,
                    getTnmPayRequestPayWaitPeriod(zeebeProperties.getWaitTnmPayRequestPeriod()));
            zeebeClient.newCreateInstanceCommand().bpmnProcessId(getWorkflowId(TNM_PAYBILL_WORKFLOW_TYPE, TNM_PAYBILL_WORKFLOW_SUBTYPE,
                    amsName, amsPayBillProps.getAccountHoldingInstitutionId())).latestVersion().variables(variables).send().join();
            zeebeClient.newPublishMessageCommand().messageName("pendingPayRequest").correlationKey(workflowInstanceKey)
                    .timeToLive(Duration.ofMillis(300)).variables(variables).send();
        }
        e.getIn().setHeader(TNM_PAY_OAF_TRANSACTION_REFERENCE, oafTransactionReference);
    }

    /**
     * Validate the unique transaction ID.
     *
     * @param transactionId
     *            the transaction ID
     * @throws JsonProcessingException
     *             if there is an error processing JSON
     */
    public void validateUniqueTransactionId(String transactionId) throws JsonProcessingException {
        log.info("Checking transaction status for transactionId: {}", transactionId);
        Exchange exchange = producerTemplate.send("direct:paybill-transaction-status-check-base",
                ex -> ex.getIn().setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, transactionId));
        String responseBody = exchange.getIn().getBody(String.class);
        if (!Objects.isNull(responseBody)) {
            TransactionStatusResponseDTO response = objectMapper.readValue(responseBody, TransactionStatusResponseDTO.class);
            if (TransferState.COMMITTED.equals(response.getTransferState())) {
                throw new TnmConnectorExistingTransactionIdException("Transaction ID already exists");
            }
        }
    }

    /**
     * Process the request for Transaction status check.
     *
     * @param exchange
     *            {@link Exchange}
     */
    public void processRequestForTransactionStatusCheck(Exchange exchange) {
        log.debug("## PayBill Transaction status check route");
        Object tnmTransactionId = exchange.getIn().getHeader(PAYBILL_TRANSACTION_ID_URL_PARAM);

        if (Objects.isNull(tnmTransactionId) || !StringUtils.hasText(tnmTransactionId.toString())) {
            throw new MissingFieldException("Transaction id is mandatory");
        }

        exchange.getIn().removeHeaders("*");
        exchange.getIn().setHeader(CONTENT_TYPE, CONTENT_TYPE_VAL);
        exchange.getIn().setHeader("requestType", "transfers");
        exchange.getIn().setHeader(TENANT_ID, tenantId);

        exchange.setProperty(PAYBILL_TRANSACTION_ID_URL_PARAM, tnmTransactionId.toString());
        exchange.setProperty(CHANNEL_URL, channelUrl);
    }

    /**
     * Process the response for PayBill validation response success.
     *
     * @param e
     *            {@link Exchange}
     */
    public void processResponseForPayBillValidationResponseSuccess(Exchange e) {
        String channelResponseBodyString = e.getIn().getBody(String.class);
        JSONObject channelResponse = new JSONObject(channelResponseBodyString);
        log.debug("channelResponse:{}", channelResponse);
        String workflowInstanceKey = channelResponse.getString("transactionId");

        // Retrieving client correlation ID added to the header in --- route
        String clientCorrelationId = e.getIn().getHeader(X_CORRELATION_ID).toString();
        Object clientName = e.getIn().getHeader(CLIENT_NAME);
        Boolean reconciled = reconciledStore.get(clientCorrelationId);
        // Storing the key value
        workflowInstanceStore.put(clientCorrelationId, workflowInstanceKey);
        reconciledStore.remove(clientCorrelationId);

        e.getIn().setBody(
                buildPayBillValidationResponse(reconciled, clientCorrelationId, Objects.nonNull(clientName) ? clientName.toString() : null)
                        .toString());
    }

    /**
     * Process the response for PayBill validation response error.
     *
     * @param e
     *            {@link Exchange}
     */
    public void processResponseForPayBillValidationResponseError(Exchange e) {
        JSONObject channelResponse = new JSONObject(e.getIn().getBody());
        log.debug("channelResponse:{}", channelResponse);

        e.getIn().setBody(buildPayBillValidationResponse(false, null, null).toString());
    }

}
