package org.mifos.connector.tnm.util;

import static org.mifos.connector.tnm.camel.config.CamelProperties.FINERACT_PRIMARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.GET_ACCOUNT_DETAILS_FLAG;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYBILL_TRANSFER_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYMENT_SCHEME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.ROSTER_PRIMARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.SECONDARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_CONSTANT;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_PAY_REQUEST_PAY_WAIT_PERIOD;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_TRX_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TNM_VALIDATION_REQUEST_PAYLOAD;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.CURRENCY;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.PARTY_LOOKUP_FAILED;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.TRANSACTION_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.mifos.connector.common.channel.dto.TransactionStatusResponseDTO;
import org.mifos.connector.common.gsma.dto.CustomData;
import org.mifos.connector.common.gsma.dto.GsmaTransfer;
import org.mifos.connector.common.gsma.dto.Party;
import org.mifos.connector.common.mojaloop.type.TransferState;
import org.mifos.connector.tnm.dto.ChannelRequestDto;
import org.mifos.connector.tnm.dto.ChannelValidationRequestDto;
import org.mifos.connector.tnm.dto.PayBillValidationResponseDto;
import org.mifos.connector.tnm.dto.TnmPayBillPayRequestDto;
import org.springframework.stereotype.Component;

/**
 * Utility class.
 */
@Slf4j
@Component
public class TnmUtils {

    private TnmUtils() {
        // Hide the constructor
    }

    /**
     * Get the object mapper.
     *
     * @return {@link ObjectMapper}
     */
    // TODO: Move to ph-ee-connector-common
    public static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }

    /**
     * Build {@link ChannelValidationRequestDto}.
     *
     * @param accountId
     *            the client account number
     * @param phoneNumber
     *            the phone number
     * @param currency
     *            the currency
     * @return {@link ChannelValidationRequestDto}
     */
    public static ChannelValidationRequestDto createValidationRequest(String accountId, String amsName, String currency, String phoneNumber,
            boolean getAccountDetailsFlag) {

        // Mapping primary and secondary Identifier
        CustomData primaryIdentifier = new CustomData();

        primaryIdentifier.setKey(getPrimaryIdentifierName(amsName));
        primaryIdentifier.setValue(accountId);

        CustomData secondaryIdentifier = new CustomData();
        secondaryIdentifier.setKey(SECONDARY_IDENTIFIER_NAME);
        secondaryIdentifier.setValue(phoneNumber);

        // Mapping custom data
        CustomData transactionId = new CustomData();
        transactionId.setKey(TRANSACTION_ID);
        transactionId.setValue(generateWorkflowId());

        CustomData currencyObj = new CustomData();
        currencyObj.setKey(CURRENCY);
        currencyObj.setValue(currency);

        CustomData getAccountDetails = new CustomData();
        getAccountDetails.setKey(GET_ACCOUNT_DETAILS_FLAG);
        getAccountDetails.setValue(getAccountDetailsFlag);

        ChannelValidationRequestDto validationRequestDto = new ChannelValidationRequestDto();
        validationRequestDto.setPrimaryIdentifier(primaryIdentifier);
        validationRequestDto.setSecondaryIdentifier(secondaryIdentifier);
        validationRequestDto.setCustomData(List.of(transactionId, currencyObj, getAccountDetails));

        return validationRequestDto;
    }

    /**
     * Convert {@link PayBillValidationResponseDto} to {@link GsmaTransfer}.
     *
     * @param paybillValidationResponseDto
     *            {@link PayBillValidationResponseDto}
     * @param clientCorrelationId
     *            the correlation id
     * @return {@link ChannelValidationRequestDto}
     */
    public static GsmaTransfer createGsmaTransferDto(PayBillValidationResponseDto paybillValidationResponseDto, String clientCorrelationId,
            int tnmpayRequestWaitPeriod) throws JsonProcessingException {

        Party payer = new Party();
        payer.setPartyIdIdentifier(paybillValidationResponseDto.getMsisdn());
        payer.setPartyIdType("MSISDN");
        List<Party> payerObj = new ArrayList<>();
        payerObj.add(payer);

        Party payee = new Party();
        payee.setPartyIdIdentifier(paybillValidationResponseDto.getMsisdn());
        payee.setPartyIdType("accountId");
        List<Party> payeeObj = new ArrayList<>();
        payeeObj.add(payee);

        GsmaTransfer gsmaTransfer = new GsmaTransfer();
        List<CustomData> customData = setCustomData(paybillValidationResponseDto, clientCorrelationId, tnmpayRequestWaitPeriod);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        String currentDateTime = formatter.format(new Date());

        gsmaTransfer.setCustomData(customData);
        gsmaTransfer.setRequestDate(currentDateTime);
        gsmaTransfer.setPayee(payeeObj);
        gsmaTransfer.setPayer(payerObj);
        gsmaTransfer.setSubType("inbound");
        gsmaTransfer.setType("tnm");
        gsmaTransfer.setDescriptionText("description");
        gsmaTransfer.setRequestingOrganisationTransactionReference(paybillValidationResponseDto.getTransactionId());
        gsmaTransfer.setAmount(paybillValidationResponseDto.getAmount());
        gsmaTransfer.setCurrency(paybillValidationResponseDto.getCurrency());

        return gsmaTransfer;
    }

    /**
     * Set custom data from {@link PayBillValidationResponseDto}.
     *
     * @param payBillValidationResponseDto
     *            {@link PayBillValidationResponseDto}
     * @param clientCorrelationId
     *            the client correlation ID
     * @param tnmpayRequestWaitPeriod
     *            the wait period for the TNM pay request
     * @return a list of {@link CustomData}
     */
    private static List<CustomData> setCustomData(PayBillValidationResponseDto payBillValidationResponseDto, String clientCorrelationId,
            int tnmpayRequestWaitPeriod) throws JsonProcessingException {
        CustomData reconciled = new CustomData();
        reconciled.setKey(PARTY_LOOKUP_FAILED);
        reconciled.setValue(!payBillValidationResponseDto.isReconciled());
        CustomData confirmationReceived = new CustomData();
        confirmationReceived.setKey("confirmationReceived");
        confirmationReceived.setValue(false);
        // This customData will be set as Zeebe variable and be used in Clearing workflow route
        CustomData txnId = new CustomData();
        txnId.setKey(TNM_TRX_ID);
        txnId.setValue(payBillValidationResponseDto.getTransactionId());
        CustomData ams = new CustomData();
        ams.setKey("ams");
        ams.setValue(payBillValidationResponseDto.getAmsName());
        CustomData tenantId = new CustomData();
        tenantId.setKey("tenantId");
        tenantId.setValue(payBillValidationResponseDto.getAccountHoldingInstitutionId());
        CustomData clientCorrelation = new CustomData();
        clientCorrelation.setKey("clientCorrelationId");
        clientCorrelation.setValue(clientCorrelationId);
        CustomData currency = new CustomData();
        currency.setKey("currency");
        currency.setValue(payBillValidationResponseDto.getCurrency());

        CustomData tmmPayRequestWaitPeriod = new CustomData();
        tmmPayRequestWaitPeriod.setKey(TNM_PAY_REQUEST_PAY_WAIT_PERIOD);
        tmmPayRequestWaitPeriod.setValue(getTnmPayRequestPayWaitPeriod(tnmpayRequestWaitPeriod));

        CustomData validationPayload = new CustomData();
        validationPayload.setKey(TNM_VALIDATION_REQUEST_PAYLOAD);
        validationPayload.setValue(getObjectMapper().writeValueAsString(payBillValidationResponseDto));

        CustomData transferCode = new CustomData();
        transferCode.setKey(PAYBILL_TRANSFER_CODE);
        transferCode.setValue(payBillValidationResponseDto.getTransactionId());

        CustomData paymentScheme = new CustomData(PAYMENT_SCHEME, TNM_CONSTANT);

        List<CustomData> customData = new ArrayList<>();
        customData.add(reconciled);
        customData.add(confirmationReceived);
        customData.add(txnId);
        customData.add(transferCode);
        customData.add(ams);
        customData.add(tenantId);
        customData.add(clientCorrelation);
        customData.add(currency);
        customData.add(tmmPayRequestWaitPeriod);
        customData.add(validationPayload);
        customData.add(paymentScheme);
        return customData;
    }

    /**
     * Convert {@link TnmPayBillPayRequestDto} to {@link ChannelRequestDto}.
     *
     * @param payBillConfirmationRequestDto
     *            {@link TnmPayBillPayRequestDto}
     * @param amsName
     *            the AMS name
     * @param currency
     *            the currency
     * @return {@link ChannelRequestDto}
     */
    public static ChannelRequestDto convertPayBillToChannelPayload(TnmPayBillPayRequestDto payBillConfirmationRequestDto, String amsName,
            String currency) {
        JSONObject payer = new JSONObject();

        JSONObject partyIdInfoPayer = new JSONObject();
        partyIdInfoPayer.put("partyIdType", SECONDARY_IDENTIFIER_NAME);
        partyIdInfoPayer.put("partyIdentifier", payBillConfirmationRequestDto.getMsisdn());

        payer.put("partyIdInfo", partyIdInfoPayer);

        JSONObject payee = new JSONObject();
        JSONObject partyIdInfoPayee = new JSONObject();

        partyIdInfoPayee.put("partyIdType", getPrimaryIdentifierName(amsName));
        partyIdInfoPayee.put("partyIdentifier", payBillConfirmationRequestDto.getAccountNumber());

        payee.put("partyIdInfo", partyIdInfoPayee);

        JSONObject amount = new JSONObject();
        amount.put("amount", payBillConfirmationRequestDto.getTransactionAmount());
        amount.put("currency", currency);

        return new ChannelRequestDto(payer, payee, amount, null, null);
    }

    /**
     * Build the workflow identifier.
     *
     * @param type
     *            the workflow type
     * @param subtype
     *            the workflow subtype
     * @param amsName
     *            the AMS name
     * @param accountHoldingInstitutionId
     *            the account holding institution ID
     * @return the workflow identifier
     */
    public static String getWorkflowId(String type, String subtype, String amsName, String accountHoldingInstitutionId) {
        return new StringBuilder().append(subtype).append("_").append(type).append("_").append(amsName).append("-")
                .append(accountHoldingInstitutionId).toString();

    }

    /**
     * Generate the transaction identifier.
     *
     * @return the transaction id
     */
    public static String generateWorkflowId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Build the PayBill validation response.
     *
     * @param reconciled
     *            the reconciled flag
     * @param transactionId
     *            the transaction id
     * @param clientName
     *            the client name
     * @return the PayBill validation response
     */
    public static JSONObject buildPayBillValidationResponse(Boolean reconciled, String transactionId, String clientName) {
        JSONObject responseObject = new JSONObject();
        boolean isReconciled = Boolean.TRUE.equals(reconciled);
        responseObject.put("status", isReconciled ? 200 : 404);
        responseObject.put("message", isReconciled ? "Account exists" : "Account does not exists or payment not allowed");
        responseObject.put("oafTransactionReference", transactionId);
        if (isReconciled) {
            responseObject.put("clientName", clientName);
        }
        return responseObject;
    }

    /**
     * Build the PayBill transaction response.
     *
     * @param successful
     *            the success flag
     * @param response
     *            the response
     * @return the transaction response as {@link JSONObject}
     */
    public static JSONObject buildPayBillTransactionResponseResponse(boolean successful, Object response) {
        JSONObject responseObject = new JSONObject();
        int status = 404;
        String message = "Transaction not found";
        if (successful && response != null && response instanceof TransactionStatusResponseDTO responseDto) {
            TransferState transferState = responseDto.getTransferState();
            log.debug("Transfer state: {}", transferState);
            if (TransferState.COMMITTED.equals(transferState)) {
                status = 200;
                message = "Payment successful";
            }
            responseObject.put("receipt_number", responseDto.getTransferId());
            responseObject.put("trans_id", responseDto.getTransactionId());
        }

        responseObject.put("status", status);
        responseObject.put("message", message);

        return responseObject;
    }

    /**
     * Get the primary identifier name.
     *
     * @param amsName
     *            the AMS name
     * @return the primary identifier name
     */
    public static String getPrimaryIdentifierName(String amsName) {
        String primaryIdentifierName = null;
        if (amsName.equalsIgnoreCase("roster")) {
            primaryIdentifierName = ROSTER_PRIMARY_IDENTIFIER_NAME;
        } else if (amsName.equalsIgnoreCase("fineract")) {
            primaryIdentifierName = FINERACT_PRIMARY_IDENTIFIER_NAME;
        }
        return primaryIdentifierName;
    }

    public static String getTnmPayRequestPayWaitPeriod(int tnmpayRequestWaitPeriod) {
        return String.format("PT%dS", tnmpayRequestWaitPeriod);
    }

}
