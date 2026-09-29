package org.mifos.connector.tnm.camel.routes;

import static org.mifos.connector.tnm.camel.config.CamelProperties.ACCOUNT_HOLDING_INSTITUTION_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.AMS_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.BUSINESS_SHORT_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CAMEL_HTTP_RESPONSE_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CHANNEL_URL;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_ACCOUNT_NUMBER;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CONTENT_TYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.GET_ACCOUNT_DETAILS_FLAG;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYBILL_TRANSACTION_ID_URL_PARAM;
import static org.mifos.connector.tnm.camel.config.CamelProperties.SECONDARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.X_CORRELATION_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.CURRENCY;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.IS_VALIDATION_REFERENCE_PRESENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.ZeebeFuture;
import io.camunda.zeebe.client.api.command.CreateProcessInstanceCommandStep1;
import io.camunda.zeebe.client.api.command.PublishMessageCommandStep1;
import java.util.UUID;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mifos.connector.tnm.camel.config.AmsPayBillProperties;
import org.mifos.connector.tnm.camel.config.AmsProperties;
import org.mifos.connector.tnm.camel.config.ZeebeProperties;
import org.mifos.connector.tnm.dto.ChannelValidationRequestDto;
import org.mifos.connector.tnm.dto.PayBillValidationResponseDto;
import org.mifos.connector.tnm.dto.TnmPayBillPayRequestDto;
import org.mifos.connector.tnm.exception.MissingFieldException;
import org.mifos.connector.tnm.exception.TnmConnectorExistingTransactionIdException;
import org.mifos.connector.tnm.exception.TnmConnectorJsonProcessingException;
import org.mifos.connector.tnm.flowcomponents.PaybillStateStore;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PayBillRouteProcessorTest {

    @Mock
    private ZeebeClient zeebeClient;
    @Mock
    private ProducerTemplate producerTemplate;
    @Mock
    private AmsPayBillProperties amsPayBillProps;
    @Mock
    private ZeebeProperties zeebeProperties;
    @Mock
    private PaybillStateStore paybillStateStore;

    private PayBillRouteProcessor processor;
    private CamelContext camelContext;

    @BeforeEach
    void setUp() throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.start();
        processor = new PayBillRouteProcessor(producerTemplate, zeebeClient, amsPayBillProps, zeebeProperties, paybillStateStore);
        ReflectionTestUtils.setField(processor, "tenantId", "malawi");
        ReflectionTestUtils.setField(processor, "channelUrl", "http://channel.test");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (camelContext != null) {
            camelContext.stop();
        }
    }

    private Exchange newExchange() {
        return new DefaultExchange(camelContext);
    }

    private AmsProperties fineractAms() {
        AmsProperties amsProperties = new AmsProperties();
        amsProperties.setAms("fineract");
        amsProperties.setCurrency("MWK");
        amsProperties.setBaseUrl("http://test.com");
        amsProperties.setBusinessShortCode("24322607");
        return amsProperties;
    }

    @DisplayName("Successfully builds account status request body with all required headers present")
    @Test
    void test_build_body_with_valid_headers() throws JsonProcessingException {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(CURRENCY, "MWK");
        exchange.getIn().setHeader(BUSINESS_SHORT_CODE, "24322607");
        exchange.getIn().setHeader(SECONDARY_IDENTIFIER_NAME, "roster");
        exchange.getIn().setHeader(GET_ACCOUNT_DETAILS_FLAG, false);

        when(amsPayBillProps.getAmsPropertiesFromShortCode(anyString())).thenReturn(fineractAms());
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("TEST_ID");

        ObjectMapper objectMapper = new ObjectMapper();
        String result = processor.buildBodyForAccountStatus(exchange);
        ChannelValidationRequestDto requestDto = objectMapper.readValue(result, ChannelValidationRequestDto.class);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("fineractAccountID", requestDto.getPrimaryIdentifier().getKey());
        Assertions.assertEquals("12345", requestDto.getPrimaryIdentifier().getValue());
        Assertions.assertEquals("transactionId", requestDto.getCustomData().get(0).key);
        Assertions.assertDoesNotThrow(() -> UUID.fromString(requestDto.getCustomData().get(0).value.toString()));
        Assertions.assertEquals("currency", requestDto.getCustomData().get(1).key);
        Assertions.assertEquals("MWK", requestDto.getCustomData().get(1).value);
        Assertions.assertEquals("getAccountDetails", requestDto.getCustomData().get(2).key);
        Assertions.assertEquals(false, requestDto.getCustomData().get(2).value);
        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("fineract", exchange.getIn().getHeader("amsName"));
        Assertions.assertEquals("http://channel.test", exchange.getIn().getHeader(CHANNEL_URL));
        Assertions.assertEquals("fineractAccountID", exchange.getIn().getHeader("primaryIdentifier"));
        Assertions.assertEquals("12345", exchange.getIn().getHeader("primaryIdentifierValue"));
    }

    @DisplayName("Successfully builds account status request body without currency and short code")
    @Test
    void test_build_body_with_valid_headers_without_currency_and_short_code() throws JsonProcessingException {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(SECONDARY_IDENTIFIER_NAME, "roster");

        when(amsPayBillProps.getAmsPropertiesFromShortCode(anyString())).thenReturn(fineractAms());
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC001");
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("TEST_ID");

        ObjectMapper objectMapper = new ObjectMapper();
        String result = processor.buildBodyForAccountStatus(exchange);
        ChannelValidationRequestDto requestDto = objectMapper.readValue(result, ChannelValidationRequestDto.class);

        Assertions.assertEquals("fineractAccountID", requestDto.getPrimaryIdentifier().getKey());
        Assertions.assertEquals(true, requestDto.getCustomData().get(2).value);
        Assertions.assertEquals("fineract", exchange.getIn().getHeader("amsName"));
    }

    @DisplayName("Handles missing MSISDN by throwing MissingFieldException")
    @Test
    void test_build_body_missing_msisdn_throws_exception() {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(CURRENCY, "USD");
        exchange.getIn().setHeader(BUSINESS_SHORT_CODE, "BSC001");
        exchange.getIn().setHeader(GET_ACCOUNT_DETAILS_FLAG, true);

        MissingFieldException exception = Assertions.assertThrows(MissingFieldException.class,
                () -> processor.buildBodyForAccountStatus(exchange));
        Assertions.assertEquals("MSISDN is required for PayBill validation", exception.getMessage());
    }

    @DisplayName("Successfully processes PayBillValidationResponseDto and returns serialized GsmaTransfer")
    @Test
    void test_successful_processing_of_payBill_validation_response() {
        PayBillValidationResponseDto validationResponseDto = new PayBillValidationResponseDto();
        validationResponseDto.setReconciled(true);
        validationResponseDto.setTransactionId("test-txn-123");
        validationResponseDto.setAccountHoldingInstitutionId("inst-123");
        validationResponseDto.setAmsName("test-ams");
        validationResponseDto.setClientName("test-client");
        validationResponseDto.setMsisdn("123456789");
        validationResponseDto.setAmount("100");
        validationResponseDto.setCurrency("USD");

        when(zeebeProperties.getWaitTnmPayRequestPeriod()).thenReturn(5);

        Exchange exchange = newExchange();
        exchange.getIn().setBody(validationResponseDto);

        String result = processor.buildBodyForStartPayBillWorkflow(exchange);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("inst-123", exchange.getIn().getHeader(ACCOUNT_HOLDING_INSTITUTION_ID));
        Assertions.assertEquals("test-ams", exchange.getIn().getHeader(AMS_NAME));
        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("malawi", exchange.getIn().getHeader(TENANT_ID));
        Assertions.assertEquals("test-txn-123", exchange.getIn().getHeader(X_CORRELATION_ID));
        Assertions.assertEquals("test-client", exchange.getIn().getHeader(CLIENT_NAME));
        Assertions.assertEquals("http://channel.test", exchange.getIn().getHeader(CHANNEL_URL));
        Assertions.assertTrue(Boolean.TRUE.equals(exchange.getProperty(IS_VALIDATION_REFERENCE_PRESENT)));
        verify(paybillStateStore, never()).putWorkflowInstance(anyString(), anyString());
    }

    @DisplayName("Start workflow with reconciled=false keeps flag on exchange and does not touch store")
    @Test
    void test_start_workflow_with_reconciled_false() {
        PayBillValidationResponseDto validationResponseDto = new PayBillValidationResponseDto();
        validationResponseDto.setReconciled(false);
        validationResponseDto.setTransactionId("txn-unreconciled");
        validationResponseDto.setAccountHoldingInstitutionId("inst-123");
        validationResponseDto.setAmsName("test-ams");
        validationResponseDto.setClientName("client");
        validationResponseDto.setMsisdn("123456789");
        validationResponseDto.setAmount("100");
        validationResponseDto.setCurrency("MWK");
        when(zeebeProperties.getWaitTnmPayRequestPeriod()).thenReturn(5);

        Exchange exchange = newExchange();
        exchange.getIn().setBody(validationResponseDto);

        processor.buildBodyForStartPayBillWorkflow(exchange);

        Assertions.assertFalse(Boolean.TRUE.equals(exchange.getProperty(IS_VALIDATION_REFERENCE_PRESENT)));
        verify(paybillStateStore, never()).putWorkflowInstance(anyString(), anyString());
    }

    @DisplayName("Successfully processes PayBill request with store mapping (publish only)")
    @Test
    void test_process_valid_paybill_requests() {
        TnmPayBillPayRequestDto requestDto = new TnmPayBillPayRequestDto();
        requestDto.setTransactionId("TEST-TXN-123");
        requestDto.setOafValidationRef("OAF-REF-123");
        requestDto.setMsisdn("123456789");
        requestDto.setTransactionAmount("100");
        requestDto.setAccountNumber("ACC123");

        AmsProperties amsProps = new AmsProperties();
        amsProps.setAms("TEST-AMS");
        amsProps.setCurrency("USD");
        amsProps.setBaseUrl("http://test-url");
        Exchange exchange = mock(Exchange.class);
        when(amsPayBillProps.getAmsPropertiesFromShortCode(any())).thenReturn(amsProps);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenAnswer(invocation -> {
            Processor processor1 = invocation.getArgument(1, Processor.class);
            processor1.process(exchange);
            return exchange;
        });

        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);

        stubPublishMessage();
        when(paybillStateStore.getWorkflowInstance(requestDto.getOafValidationRef())).thenReturn("TEST-INSTANCE-123");

        processor.processRequestForPayBillPayRoute(exchange);

        verify(paybillStateStore).getWorkflowInstance("OAF-REF-123");
        verify(zeebeClient).newPublishMessageCommand();
        verify(zeebeClient, never()).newCreateInstanceCommand();
    }

    @DisplayName("Pay without store mapping creates a new workflow instance")
    @Test
    void test_process_paybill_without_store_mapping_creates_instance() {
        TnmPayBillPayRequestDto requestDto = new TnmPayBillPayRequestDto();
        requestDto.setTransactionId("TEST-TXN-NO-STORE");
        requestDto.setOafValidationRef("OAF-MISSING");
        requestDto.setMsisdn("123456789");
        requestDto.setTransactionAmount("100");
        requestDto.setAccountNumber("ACC123");

        AmsProperties amsProps = new AmsProperties();
        amsProps.setAms("TEST-AMS");
        amsProps.setCurrency("USD");
        amsProps.setBaseUrl("http://test-url");
        when(amsPayBillProps.getAmsPropertiesFromShortCode(any())).thenReturn(amsProps);
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("malawi");
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC");
        when(zeebeProperties.getWaitTnmPayRequestPeriod()).thenReturn(60);
        when(paybillStateStore.getWorkflowInstance("OAF-MISSING")).thenReturn(null);

        Exchange exchange = mock(Exchange.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenAnswer(invocation -> {
            Processor processor1 = invocation.getArgument(1, Processor.class);
            processor1.process(exchange);
            return exchange;
        });
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);

        stubCreateInstance();
        stubPublishMessage();

        processor.processRequestForPayBillPayRoute(exchange);

        verify(paybillStateStore).getWorkflowInstance("OAF-MISSING");
        verify(zeebeClient).newCreateInstanceCommand();
        verify(zeebeClient).newPublishMessageCommand();
    }

    @DisplayName("Pay without validation reference creates workflow when store miss")
    @Test
    void test_process_paybill_without_oaf_validation_ref() {
        TnmPayBillPayRequestDto requestDto = new TnmPayBillPayRequestDto();
        requestDto.setTransactionId("TEST-TXN-NO-REF");
        requestDto.setOafValidationRef(null);
        requestDto.setMsisdn("123456789");
        requestDto.setTransactionAmount("50");
        requestDto.setAccountNumber("ACC999");

        AmsProperties amsProps = new AmsProperties();
        amsProps.setAms("TEST-AMS");
        amsProps.setCurrency("MWK");
        amsProps.setBaseUrl("http://test-url");
        when(amsPayBillProps.getAmsPropertiesFromShortCode(any())).thenReturn(amsProps);
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("malawi");
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC");
        when(zeebeProperties.getWaitTnmPayRequestPeriod()).thenReturn(60);
        when(paybillStateStore.getWorkflowInstance(null)).thenReturn(null);

        Exchange exchange = mock(Exchange.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenAnswer(invocation -> {
            Processor processor1 = invocation.getArgument(1, Processor.class);
            processor1.process(exchange);
            return exchange;
        });
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);

        stubCreateInstance();
        stubPublishMessage();

        processor.processRequestForPayBillPayRoute(exchange);

        verify(paybillStateStore).getWorkflowInstance(null);
        verify(zeebeClient).newCreateInstanceCommand();
    }

    @DisplayName("Validation success with reconciled=false still stores workflow instance")
    @Test
    void test_validation_response_success_with_reconciled_false_still_puts_workflow() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody("{\"transactionId\":\"wf-1\"}");
        exchange.getIn().setHeader(X_CORRELATION_ID, "corr-false");
        exchange.getIn().setHeader(CLIENT_NAME, "Jane");
        exchange.setProperty(IS_VALIDATION_REFERENCE_PRESENT, false);

        processor.processResponseForPayBillValidationResponseSuccess(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        Assertions.assertEquals(404, response.getInt("status"));
        verify(paybillStateStore).putWorkflowInstance("corr-false", "wf-1");
    }

    @DisplayName("Validation success with null client name still puts workflow")
    @Test
    void test_validation_response_success_with_null_client_name() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody("{\"transactionId\":\"wf-2\"}");
        exchange.getIn().setHeader(X_CORRELATION_ID, "corr-2");
        exchange.setProperty(IS_VALIDATION_REFERENCE_PRESENT, true);

        processor.processResponseForPayBillValidationResponseSuccess(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        Assertions.assertEquals(200, response.getInt("status"));
        Assertions
                .assertTrue(response.isNull("clientName") || !response.has("clientName") || response.optString("clientName", "").isEmpty());
        verify(paybillStateStore).putWorkflowInstance("corr-2", "wf-2");
    }

    @DisplayName("Validation success rejects null channel body")
    @Test
    void test_validation_response_success_rejects_null_body() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody(null);
        exchange.getIn().setHeader(CAMEL_HTTP_RESPONSE_CODE, 502);

        TnmConnectorJsonProcessingException ex = Assertions.assertThrows(TnmConnectorJsonProcessingException.class,
                () -> processor.processResponseForPayBillValidationResponseSuccess(exchange));
        Assertions.assertTrue(ex.getMessage().contains("httpStatus=502"));
        verify(paybillStateStore, never()).putWorkflowInstance(anyString(), anyString());
    }

    @DisplayName("Validation success rejects non-JSON channel body and uses snippet")
    @Test
    void test_validation_response_success_rejects_non_json_body() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody("  Internal Server Error from gateway");
        exchange.getIn().setHeader(CAMEL_HTTP_RESPONSE_CODE, 500);

        Assertions.assertThrows(TnmConnectorJsonProcessingException.class,
                () -> processor.processResponseForPayBillValidationResponseSuccess(exchange));
        verify(paybillStateStore, never()).putWorkflowInstance(anyString(), anyString());
    }

    @DisplayName("Validation success rejects empty channel body")
    @Test
    void test_validation_response_success_rejects_empty_body() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody("   ");
        exchange.getIn().setHeader(CAMEL_HTTP_RESPONSE_CODE, 200);

        Assertions.assertThrows(TnmConnectorJsonProcessingException.class,
                () -> processor.processResponseForPayBillValidationResponseSuccess(exchange));
    }

    @DisplayName("Validation success rejects long non-JSON body (snippet truncation path)")
    @Test
    void test_validation_response_success_rejects_long_non_json_body() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody("x".repeat(250));
        exchange.getIn().setHeader(CAMEL_HTTP_RESPONSE_CODE, 503);

        Assertions.assertThrows(TnmConnectorJsonProcessingException.class,
                () -> processor.processResponseForPayBillValidationResponseSuccess(exchange));
    }

    @DisplayName("Pay route uses currency and short-code headers when present")
    @Test
    void test_process_paybill_uses_headers_when_present() {
        TnmPayBillPayRequestDto requestDto = new TnmPayBillPayRequestDto();
        requestDto.setTransactionId("TEST-TXN-HDR");
        requestDto.setOafValidationRef("OAF-HDR");
        requestDto.setMsisdn("123456789");
        requestDto.setTransactionAmount("100");
        requestDto.setAccountNumber("ACC123");

        AmsProperties amsProps = new AmsProperties();
        amsProps.setAms("TEST-AMS");
        amsProps.setCurrency("USD");
        amsProps.setBaseUrl("http://test-url");
        when(amsPayBillProps.getAmsPropertiesFromShortCode("24322607")).thenReturn(amsProps);
        when(paybillStateStore.getWorkflowInstance("OAF-HDR")).thenReturn("WF-HDR");

        Exchange exchange = mock(Exchange.class);
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);
        when(message.getHeader(CURRENCY)).thenReturn("MWK");
        when(message.getHeader(BUSINESS_SHORT_CODE)).thenReturn("24322607");
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenAnswer(invocation -> {
            Processor processor1 = invocation.getArgument(1, Processor.class);
            processor1.process(exchange);
            return exchange;
        });
        stubPublishMessage();

        processor.processRequestForPayBillPayRoute(exchange);

        verify(amsPayBillProps).getAmsPropertiesFromShortCode("24322607");
        verify(zeebeClient).newPublishMessageCommand();
        verify(zeebeClient, never()).newCreateInstanceCommand();
    }

    @DisplayName("Pay route wraps JsonProcessingException from status check")
    @Test
    void test_process_paybill_wraps_json_processing_exception() throws Exception {
        TnmPayBillPayRequestDto requestDto = new TnmPayBillPayRequestDto();
        requestDto.setTransactionId("TEST-TXN-JSON");
        requestDto.setOafValidationRef("OAF-JSON");
        requestDto.setMsisdn("123456789");
        requestDto.setTransactionAmount("100");
        requestDto.setAccountNumber("ACC123");

        AmsProperties amsProps = new AmsProperties();
        amsProps.setAms("TEST-AMS");
        amsProps.setCurrency("USD");
        amsProps.setBaseUrl("http://test-url");
        when(amsPayBillProps.getAmsPropertiesFromShortCode(any())).thenReturn(amsProps);
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC");

        Exchange statusExchange = mock(Exchange.class);
        Message statusMessage = mock(Message.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(statusExchange);
        when(statusExchange.getIn()).thenReturn(statusMessage);
        when(statusMessage.getBody(String.class)).thenReturn("{bad-json");

        Exchange exchange = mock(Exchange.class);
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);

        Assertions.assertThrows(TnmConnectorJsonProcessingException.class, () -> processor.processRequestForPayBillPayRoute(exchange));
    }

    @DisplayName("buildBodyForAccountStatus wraps ObjectMapper failures")
    @Test
    void test_build_body_wraps_object_mapper_failure() throws Exception {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(SECONDARY_IDENTIFIER_NAME, "roster");
        when(amsPayBillProps.getAmsPropertiesFromShortCode(any())).thenReturn(fineractAms());
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC");
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("TEST_ID");

        ObjectMapper failingMapper = mock(ObjectMapper.class);
        when(failingMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("boom") {});
        ReflectionTestUtils.setField(processor, "objectMapper", failingMapper);

        Assertions.assertThrows(TnmConnectorJsonProcessingException.class, () -> processor.buildBodyForAccountStatus(exchange));
    }

    @DisplayName("buildBodyForStartPayBillWorkflow wraps ObjectMapper failures")
    @Test
    void test_start_workflow_wraps_object_mapper_failure() throws Exception {
        PayBillValidationResponseDto validationResponseDto = new PayBillValidationResponseDto();
        validationResponseDto.setReconciled(true);
        validationResponseDto.setTransactionId("txn-map");
        validationResponseDto.setAccountHoldingInstitutionId("inst");
        validationResponseDto.setAmsName("ams");
        validationResponseDto.setClientName("client");
        validationResponseDto.setMsisdn("1");
        validationResponseDto.setAmount("1");
        validationResponseDto.setCurrency("MWK");
        when(zeebeProperties.getWaitTnmPayRequestPeriod()).thenReturn(5);

        ObjectMapper failingMapper = mock(ObjectMapper.class);
        when(failingMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("boom") {});
        ReflectionTestUtils.setField(processor, "objectMapper", failingMapper);

        Exchange exchange = newExchange();
        exchange.getIn().setBody(validationResponseDto);

        Assertions.assertThrows(TnmConnectorJsonProcessingException.class, () -> processor.buildBodyForStartPayBillWorkflow(exchange));
    }

    @DisplayName("Validate transaction ID that does not exist in the system")
    @Test
    void test_validate_non_existing_transaction_id() {
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn(null);

        Assertions.assertDoesNotThrow(() -> processor.validateUniqueTransactionId("test-123"));
    }

    @DisplayName("Handle JsonProcessingException during response deserialization")
    @Test
    void test_handle_json_processing_exception() {
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn("invalid-json");

        Assertions.assertThrows(JsonProcessingException.class, () -> processor.validateUniqueTransactionId("test-123"));
    }

    @Test
    void test_committed_transfer_state() {
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn("{\"transferState\":\"COMMITTED\"}");

        Assertions.assertThrows(TnmConnectorExistingTransactionIdException.class, () -> processor.validateUniqueTransactionId("test-123"));
    }

    @DisplayName("Process response with non-COMMITTED transfer state")
    @Test
    void test_non_committed_transfer_state() {
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn("{\"transferState\":\"RECEIVED\"}");

        Assertions.assertDoesNotThrow(() -> processor.validateUniqueTransactionId("test-123"));
    }

    @DisplayName("Valid transaction ID in header leads to successful processing")
    @Test
    void test_valid_transaction_id_processing() {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, "valid-transaction-id");

        processor.processRequestForTransactionStatusCheck(exchange);

        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("transfers", exchange.getIn().getHeader("requestType"));
        Assertions.assertEquals("malawi", exchange.getIn().getHeader(TENANT_ID));
        Assertions.assertEquals("valid-transaction-id", exchange.getIn().getHeader(PAYBILL_TRANSACTION_ID_URL_PARAM));
        Assertions.assertEquals("http://channel.test", exchange.getIn().getHeader(CHANNEL_URL));
        Assertions.assertEquals("valid-transaction-id", exchange.getProperty(PAYBILL_TRANSACTION_ID_URL_PARAM));
        Assertions.assertEquals("http://channel.test", exchange.getProperty(CHANNEL_URL));
    }

    @DisplayName("Null transaction ID in header throws MissingFieldException")
    @Test
    void test_null_transaction_id_throws_exception() {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, null);

        Assertions.assertThrows(MissingFieldException.class, () -> processor.processRequestForTransactionStatusCheck(exchange));
    }

    @DisplayName("Empty transaction ID in header throws MissingFieldException")
    @Test
    void test_empty_transaction_id_throws_exception() {
        Exchange exchange = newExchange();
        exchange.getIn().setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, "");

        Assertions.assertThrows(MissingFieldException.class, () -> processor.processRequestForTransactionStatusCheck(exchange));
    }

    @DisplayName("Successfully processes PayBill validation response with valid channel response")
    @Test
    void test_successful_payBill_validation_response_processing() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody("{\"transactionId\":\"123\"}");
        exchange.getIn().setHeader(X_CORRELATION_ID, "corr-123");
        exchange.getIn().setHeader(CLIENT_NAME, "John Doe");
        exchange.setProperty(IS_VALIDATION_REFERENCE_PRESENT, true);

        processor.processResponseForPayBillValidationResponseSuccess(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        Assertions.assertEquals(200, response.getInt("status"));
        Assertions.assertEquals("Account exists", response.getString("message"));
        Assertions.assertEquals("corr-123", response.getString("oafTransactionReference"));
        Assertions.assertEquals("John Doe", response.getString("clientName"));
        verify(paybillStateStore).putWorkflowInstance("corr-123", "123");
    }

    @DisplayName("Processes error response")
    @Test
    void test_process_error_response_creates_validation_response() {
        Exchange exchange = newExchange();
        exchange.getIn().setBody(new JSONObject().put("error", "Some error").toString());

        processor.processResponseForPayBillValidationResponseError(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        Assertions.assertEquals(404, response.getInt("status"));
        Assertions.assertEquals("Account does not exists or payment not allowed", response.getString("message"));
    }

    private void stubPublishMessage() {
        PublishMessageCommandStep1.PublishMessageCommandStep2 publishMessageCommandStep2 = mock(
                PublishMessageCommandStep1.PublishMessageCommandStep2.class);
        PublishMessageCommandStep1.PublishMessageCommandStep3 publishMessageCommandStep3 = mock(
                PublishMessageCommandStep1.PublishMessageCommandStep3.class);
        PublishMessageCommandStep1 publishMessageCommand = mock(PublishMessageCommandStep1.class);
        when(zeebeClient.newPublishMessageCommand()).thenReturn(publishMessageCommand);
        when(publishMessageCommand.messageName(anyString())).thenReturn(publishMessageCommandStep2);
        when(publishMessageCommandStep2.correlationKey(anyString())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.timeToLive(any())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.variables(anyMap())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.send()).thenReturn(mock(ZeebeFuture.class));
    }

    private void stubCreateInstance() {
        CreateProcessInstanceCommandStep1 createProcessInstanceCommand = mock(CreateProcessInstanceCommandStep1.class);
        CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep2 createProcessInstanceCommandStep2 = mock(
                CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep2.class);
        CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 createProcessInstanceCommandStep3 = mock(
                CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3.class);
        when(zeebeClient.newCreateInstanceCommand()).thenReturn(createProcessInstanceCommand);
        when(createProcessInstanceCommand.bpmnProcessId(anyString())).thenReturn(createProcessInstanceCommandStep2);
        when(createProcessInstanceCommandStep2.latestVersion()).thenReturn(createProcessInstanceCommandStep3);
        when(createProcessInstanceCommandStep3.variables(anyMap())).thenReturn(createProcessInstanceCommandStep3);
        when(createProcessInstanceCommandStep3.send()).thenReturn(mock(ZeebeFuture.class));
    }
}
