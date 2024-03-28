package org.mifos.connector.tnm.camel.routes;

import static org.mifos.connector.tnm.camel.config.CamelProperties.ACCOUNT_HOLDING_INSTITUTION_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.AMS_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.BUSINESS_SHORT_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CHANNEL_REQUEST;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CHANNEL_URL;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_ACCOUNT_NUMBER;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_CORRELATION_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CONTENT_TYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CONTENT_TYPE_VAL;
import static org.mifos.connector.tnm.camel.config.CamelProperties.GET_ACCOUNT_DETAILS_FLAG;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYBILL_TRANSACTION_ID_URL_PARAM;
import static org.mifos.connector.tnm.camel.config.CamelProperties.SECONDARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_PAYBILL_WORKFLOW_SUBTYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_PAYBILL_WORKFLOW_TYPE;
import static org.mifos.connector.tnm.util.TnmUtils.buildPayBillValidationResponse;
import static org.mifos.connector.tnm.util.TnmUtils.generateTransactionId;
import static org.mifos.connector.tnm.util.TnmUtils.getPrimaryIdentifierName;
import static org.mifos.connector.tnm.util.TnmUtils.getWorkflowId;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.CURRENCY;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.EXTERNAL_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.IS_VALIDATION_REFERENCE_PRESENT;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.SERVER_TRANSACTION_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSFER_CREATE_FAILED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
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
import org.json.JSONObject;
import org.mifos.connector.common.gsma.dto.GsmaTransfer;
import org.mifos.connector.tnm.camel.config.AmsPayBillProperties;
import org.mifos.connector.tnm.camel.config.AmsProperties;
import org.mifos.connector.tnm.camel.config.ZeebeProperties;
import org.mifos.connector.tnm.dto.ChannelRequestDto;
import org.mifos.connector.tnm.dto.ChannelValidationRequestDto;
import org.mifos.connector.tnm.dto.PayBillValidationResponseDto;
import org.mifos.connector.tnm.dto.TnmPayBillPayRequestDto;
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

    private final ZeebeClient zeebeClient;
    private final AmsPayBillProperties amsPayBillProps;

    private final ZeebeProperties zeebeProperties;

    @Value("${channel.host}")
    private String channelUrl;

    private ObjectMapper objectMapper = TnmUtils.getObjectMapper();
    public static Map<String, Boolean> reconciledStore = new HashMap<>();
    public static Map<String, String> workflowInstanceStore = new HashMap<>();

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
        final boolean getAccountDetailsFlag = !Objects.nonNull(getAccountDetails)
                || Boolean.parseBoolean(getAccountDetails.toString());

        if (Objects.isNull(msisdn)) {
            throw new RuntimeException("MSISDN is required for PayBill validation");
        }

        AmsProperties amsProperties = amsPayBillProps
                .getAmsPropertiesFromShortCode(Objects.nonNull(shortCodeFromReq) ? shortCodeFromReq.toString()
                        : amsPayBillProps.getDefaultAmsShortCode());
        final String amsName = amsProperties.getAms();
        final String currency = Objects.nonNull(currencyFromHeaders) ? currencyFromHeaders.toString()
                : amsProperties.getCurrency();
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
        ChannelValidationRequestDto obj = TnmUtils.createValidationRequest(clientAccountNumber, amsName, currency,
                msisdn.toString(), getAccountDetailsFlag);
        log.debug("Header:{}", exchange.getIn().getHeaders());
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
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

        GsmaTransfer gsmaTransfer = TnmUtils.createGsmaTransferDto(validationResponseDto, clientCorrelationId,
                zeebeProperties.getWaitTnmPayRequestPeriod());

        e.getIn().removeHeaders("*");
        e.getIn().setHeader(ACCOUNT_HOLDING_INSTITUTION_ID, validationResponseDto.getAccountHoldingInstitutionId());
        e.getIn().setHeader(AMS_NAME, validationResponseDto.getAmsName());
        e.getIn().setHeader(TENANT_ID, validationResponseDto.getAccountHoldingInstitutionId());
        e.getIn().setHeader(CLIENT_CORRELATION_ID, clientCorrelationId);
        e.getIn().setHeader(CONTENT_TYPE, CONTENT_TYPE_VAL);
        e.getIn().setHeader(CLIENT_NAME, validationResponseDto.getClientName());

        e.setProperty("isValidationReferencePresent", isReconciled);
        e.setProperty("channelUrl", channelUrl);
        try {
            return objectMapper.writeValueAsString(gsmaTransfer);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
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

        String shortCodeFromReq = e.getIn().getHeader(BUSINESS_SHORT_CODE).toString();
        String currencyFromHeaders = e.getIn().getHeader(CURRENCY).toString();

        AmsProperties amsProperties = amsPayBillProps.getAmsPropertiesFromShortCode(
                Objects.nonNull(shortCodeFromReq) ? shortCodeFromReq : amsPayBillProps.getDefaultAmsShortCode());
        final String amsName = amsProperties.getAms();
        final String currency = Objects.nonNull(currencyFromHeaders) ? currencyFromHeaders
                : amsProperties.getCurrency();
        final String amsUrl = amsProperties.getBaseUrl();

        Boolean isReconciled = Objects.nonNull(requestDto.getOafValidationRef());
        final String oafTransactionReference = requestDto.getOafValidationRef();
        requestDto.setValidationReferencePresent(isReconciled);
        requestDto.setAmsName(amsProperties.getAms());

        e.setProperty("amsUrl", amsUrl);
        e.setProperty("secondaryIdentifier", "MSISDN");
        e.setProperty("secondaryIdentifierValue", requestDto.getMsisdn());

        Gson gson = new Gson();
        ChannelRequestDto channelRequestDto = TnmUtils.convertPayBillToChannelPayload(requestDto, amsName, currency);
        String channelRequestDtoString = gson.toJson(channelRequestDto);
        e.setProperty("PAY_REQUEST", channelRequestDtoString);

        e.setProperty(CHANNEL_REQUEST, channelRequestDto);
        e.setProperty(EXTERNAL_ID, requestDto.getTransactionId());

        Map<String, Object> variables = new HashMap<>();
        variables.put("confirmationReceived", true);
        variables.put(CHANNEL_REQUEST, channelRequestDtoString);
        variables.put("amount", requestDto.getTransactionAmount());
        variables.put("accountId", requestDto.getAccountNumber());
        variables.put("originDate",
                Long.parseLong(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))));
        variables.put("phoneNumber", requestDto.getMsisdn());
        variables.put(SERVER_TRANSACTION_ID, requestDto.getTransactionId());

        // Getting TNM workflow transaction id and removing key
        String transactionId = workflowInstanceStore.get(oafTransactionReference);

        variables.put(IS_VALIDATION_REFERENCE_PRESENT, isReconciled && transactionId != null);
        variables.put(TRANSACTION_ID, oafTransactionReference);
        variables.put(TRANSFER_CREATE_FAILED, false);
        log.info("Workflow transaction id : {}", transactionId);

        if (transactionId != null) {
            zeebeClient.newPublishMessageCommand().messageName("pendingPayRequest").correlationKey(transactionId)
                    .timeToLive(Duration.ofMillis(300)).variables(variables).send();
            log.debug("Published Variables");
        } else {
            log.debug("No workflow of such transaction ID exists");
            transactionId = generateTransactionId();
            variables.put(ZeebeVariables.TRANSACTION_ID, transactionId);
            variables.put(ZeebeVariables.ORIGIN_DATE, Instant.now().toEpochMilli());
            zeebeClient.newCreateInstanceCommand()
                    .bpmnProcessId(getWorkflowId(TNM_PAYBILL_WORKFLOW_TYPE, TNM_PAYBILL_WORKFLOW_SUBTYPE, amsName,
                            amsPayBillProps.getAccountHoldingInstitutionId()))
                    .latestVersion().variables(variables).send().join();
            zeebeClient.newPublishMessageCommand().messageName("pendingPayRequest").correlationKey(transactionId)
                    .timeToLive(Duration.ofMillis(300)).variables(variables).send();
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
            throw new RuntimeException("Transaction id is mandatory");
        }

        exchange.getIn().removeHeaders("*");
        exchange.getIn().setHeader(CONTENT_TYPE, CONTENT_TYPE_VAL);
        exchange.getIn().setHeader("requestType", "transfers");
        exchange.getIn().setHeader(TENANT_ID, "oaf");

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
        String clientCorrelationId = e.getIn().getHeader(CLIENT_CORRELATION_ID).toString();
        Object clientName = e.getIn().getHeader(CLIENT_NAME);
        Boolean reconciled = reconciledStore.get(clientCorrelationId);
        // Storing the key value
        workflowInstanceStore.put(clientCorrelationId, workflowInstanceKey);
        reconciledStore.remove(clientCorrelationId);

        e.getIn().setBody(buildPayBillValidationResponse(reconciled, clientCorrelationId,
                Objects.nonNull(clientName) ? clientName.toString() : null).toString());
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
