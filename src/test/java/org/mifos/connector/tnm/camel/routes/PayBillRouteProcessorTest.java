package org.mifos.connector.tnm.camel.routes;

import static org.mifos.connector.tnm.camel.config.CamelProperties.ACCOUNT_HOLDING_INSTITUTION_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.AMS_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.BUSINESS_SHORT_CODE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_ACCOUNT_NUMBER;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CLIENT_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.CONTENT_TYPE;
import static org.mifos.connector.tnm.camel.config.CamelProperties.GET_ACCOUNT_DETAILS_FLAG;
import static org.mifos.connector.tnm.camel.config.CamelProperties.PAYBILL_TRANSACTION_ID_URL_PARAM;
import static org.mifos.connector.tnm.camel.config.CamelProperties.SECONDARY_IDENTIFIER_NAME;
import static org.mifos.connector.tnm.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.tnm.camel.config.CamelProperties.X_CORRELATION_ID;
import static org.mifos.connector.tnm.zeebe.ZeebeVariables.CURRENCY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.ZeebeFuture;
import io.camunda.zeebe.client.api.command.CreateProcessInstanceCommandStep1;
import io.camunda.zeebe.client.api.command.PublishMessageCommandStep1;
import io.camunda.zeebe.client.api.response.PublishMessageResponse;
import java.util.UUID;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mifos.connector.tnm.ConnectorTemplateApplicationTests;
import org.mifos.connector.tnm.camel.config.AmsPayBillProperties;
import org.mifos.connector.tnm.camel.config.AmsProperties;
import org.mifos.connector.tnm.camel.config.ZeebeProperties;
import org.mifos.connector.tnm.dto.ChannelValidationRequestDto;
import org.mifos.connector.tnm.dto.PayBillValidationResponseDto;
import org.mifos.connector.tnm.dto.TnmPayBillPayRequestDto;
import org.mifos.connector.tnm.exception.MissingFieldException;
import org.mifos.connector.tnm.exception.TnmConnectorExistingTransactionIdException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

class PayBillRouteProcessorTest extends ConnectorTemplateApplicationTests {

    @Mock
    private ZeebeClient zeebeClient;

    private PayBillRouteProcessor processor;
    @Mock
    private ProducerTemplate producerTemplate;
    @Mock
    private AmsPayBillProperties amsPayBillProps;
    @Mock
    private ZeebeProperties zeebeProperties;

    @Autowired
    private CamelContext camelContext;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        processor = new PayBillRouteProcessor(producerTemplate, zeebeClient, amsPayBillProps, zeebeProperties);
        ReflectionTestUtils.setField(processor, "tenantId", "malawi");
    }

    @DisplayName("Successfully builds account status request body with all required headers present")
    @Test
    void test_build_body_with_valid_headers() throws JsonProcessingException {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(CURRENCY, "MWK");
        exchange.getIn().setHeader(BUSINESS_SHORT_CODE, "24322607");
        exchange.getIn().setHeader(SECONDARY_IDENTIFIER_NAME, "roster");
        exchange.getIn().setHeader(GET_ACCOUNT_DETAILS_FLAG, false);

        AmsProperties amsProperties = new AmsProperties();
        amsProperties.setAms("fineract");
        amsProperties.setCurrency("MWK");
        amsProperties.setBaseUrl("http://test.com");
        amsProperties.setBusinessShortCode("24322607");

        when(amsPayBillProps.getAmsPropertiesFromShortCode(anyString())).thenReturn(amsProperties);
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC001");
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC001");
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("TEST_ID");

        ObjectMapper objectMapper = new ObjectMapper();
        String result = processor.buildBodyForAccountStatus(exchange);
        ChannelValidationRequestDto requestDto = objectMapper.readValue(result, ChannelValidationRequestDto.class);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("fineractAccountID", requestDto.getPrimaryIdentifier().getKey());
        Assertions.assertEquals("12345", requestDto.getPrimaryIdentifier().getValue());
        Assertions.assertEquals("transactionId", requestDto.getCustomData().get(0).key);
        Assertions.assertDoesNotThrow(() -> UUID.fromString(requestDto.getCustomData().get(0).value.toString()),
                "transaction id is not a valid UUID");
        Assertions.assertDoesNotThrow(() -> UUID.fromString(requestDto.getCustomData().get(0).value.toString()),
                "transaction id is not a valid UUID");
        Assertions.assertEquals("currency", requestDto.getCustomData().get(1).key);
        Assertions.assertEquals("MWK", requestDto.getCustomData().get(1).value);
        Assertions.assertEquals("getAccountDetails", requestDto.getCustomData().get(2).key);
        Assertions.assertEquals(false, requestDto.getCustomData().get(2).value);
        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("fineract", exchange.getIn().getHeader("amsName"));
    }

    @DisplayName("Successfully builds account status request body with all required headers present but "
            + "without the currency and short code")
    @Test
    void test_build_body_with_valid_headers_without_currency_and_short_code() throws JsonProcessingException {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(SECONDARY_IDENTIFIER_NAME, "roster");
        AmsProperties amsProperties = new AmsProperties();
        amsProperties.setAms("fineract");
        amsProperties.setCurrency("MWK");
        amsProperties.setBaseUrl("http://test.com");
        amsProperties.setBusinessShortCode("24322607");

        when(amsPayBillProps.getAmsPropertiesFromShortCode(anyString())).thenReturn(amsProperties);
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC001");
        when(amsPayBillProps.getDefaultAmsShortCode()).thenReturn("BSC001");
        when(amsPayBillProps.getAccountHoldingInstitutionId()).thenReturn("TEST_ID");

        ObjectMapper objectMapper = new ObjectMapper();
        String result = processor.buildBodyForAccountStatus(exchange);
        ChannelValidationRequestDto requestDto = objectMapper.readValue(result, ChannelValidationRequestDto.class);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("fineractAccountID", requestDto.getPrimaryIdentifier().getKey());
        Assertions.assertEquals("12345", requestDto.getPrimaryIdentifier().getValue());
        Assertions.assertEquals("transactionId", requestDto.getCustomData().get(0).key);
        Assertions.assertDoesNotThrow(() -> UUID.fromString(requestDto.getCustomData().get(0).value.toString()),
                "transaction id is not a valid UUID");
        Assertions.assertEquals("currency", requestDto.getCustomData().get(1).key);
        Assertions.assertEquals("MWK", requestDto.getCustomData().get(1).value);
        Assertions.assertEquals("getAccountDetails", requestDto.getCustomData().get(2).key);
        Assertions.assertEquals(true, requestDto.getCustomData().get(2).value);
        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("fineract", exchange.getIn().getHeader("amsName"));
    }

    @DisplayName("Handles missing MSISDN by throwing MissingFieldException")
    @Test
    void test_build_body_missing_msisdn_throws_exception() {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        exchange.getIn().setHeader(CLIENT_ACCOUNT_NUMBER, "12345");
        exchange.getIn().setHeader(CURRENCY, "USD");
        exchange.getIn().setHeader(BUSINESS_SHORT_CODE, "BSC001");
        // MSISDN header intentionally omitted
        exchange.getIn().setHeader(GET_ACCOUNT_DETAILS_FLAG, true);

        AmsProperties amsProperties = new AmsProperties();
        amsProperties.setAms("TEST_AMS");
        amsProperties.setCurrency("USD");
        amsProperties.setBaseUrl("http://test.com");

        when(amsPayBillProps.getAmsPropertiesFromShortCode(anyString())).thenReturn(amsProperties);

        MissingFieldException exception = Assertions.assertThrows(MissingFieldException.class, () -> {
            processor.buildBodyForAccountStatus(exchange);
        });
        Assertions.assertEquals("MSISDN is required for PayBill validation", exception.getMessage());

    }

    @DisplayName("Successfully processes PayBillValidationResponseDto and returns serialized GsmaTransfer")
    @Test
    void test_successful_processing_of_payBill_validation_response() {
        // Arrange
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

        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();
        exchange.getIn().setBody(validationResponseDto);

        // Act
        String result = processor.buildBodyForStartPayBillWorkflow(exchange);

        // Assert
        Assertions.assertNotNull(result);
        Assertions.assertEquals("inst-123", exchange.getIn().getHeader(ACCOUNT_HOLDING_INSTITUTION_ID));
        Assertions.assertEquals("test-ams", exchange.getIn().getHeader(AMS_NAME));
        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("malawi", exchange.getIn().getHeader(TENANT_ID));
        Assertions.assertEquals("test-txn-123", exchange.getIn().getHeader(X_CORRELATION_ID));
        Assertions.assertEquals("test-client", exchange.getIn().getHeader(CLIENT_NAME));
        Assertions.assertTrue((Boolean) exchange.getProperty("isValidationReferencePresent"));
    }

    @DisplayName("Successfully processes PayBill request with valid transaction ID and OAF reference")
    @Test
    void test_process_valid_paybill_request() {
        // Arrange

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
        when(amsPayBillProps.getAmsPropertiesFromShortCode(any())).thenReturn(amsProps);
        when(zeebeClient.newPublishMessageCommand()).thenReturn(mock(PublishMessageCommandStep1.class));
        Exchange exchange = mock(Exchange.class);
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenAnswer(invocation -> {
            Processor processor1 = invocation.getArgument(1, Processor.class);
            processor1.process(exchange);
            return exchange;
        });
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);

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

        PublishMessageCommandStep1.PublishMessageCommandStep2 publishMessageCommandStep2 = mock(
                PublishMessageCommandStep1.PublishMessageCommandStep2.class);

        PublishMessageCommandStep1.PublishMessageCommandStep3 publishMessageCommandStep3 = mock(
                PublishMessageCommandStep1.PublishMessageCommandStep3.class);

        ZeebeFuture<PublishMessageResponse> zeebeFutureMock = mock(ZeebeFuture.class);
        PublishMessageCommandStep1 publishMessageCommand = mock(PublishMessageCommandStep1.class);

        when(zeebeClient.newPublishMessageCommand()).thenReturn(publishMessageCommand);
        when(publishMessageCommand.messageName(anyString())).thenReturn(publishMessageCommandStep2);
        when(publishMessageCommandStep2.correlationKey(anyString())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.timeToLive(any())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.variables(anyMap())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.send()).thenReturn(zeebeFutureMock);

        // Act
        processor.processRequestForPayBillPayRoute(exchange);

        // Assert
        verify(zeebeClient).newPublishMessageCommand();
        verify(zeebeClient).newCreateInstanceCommand();
    }

    @DisplayName("Successfully processes PayBill request with valid transaction ID and OAF reference")
    @Test
    void test_process_valid_paybill_requests() {
        // Arrange

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
        when(zeebeClient.newPublishMessageCommand()).thenReturn(mock(PublishMessageCommandStep1.class));
        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenAnswer(invocation -> {
            Processor processor1 = invocation.getArgument(1, Processor.class);
            processor1.process(exchange);
            return exchange;
        });

        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(TnmPayBillPayRequestDto.class)).thenReturn(requestDto);

        PublishMessageCommandStep1.PublishMessageCommandStep2 publishMessageCommandStep2 = mock(
                PublishMessageCommandStep1.PublishMessageCommandStep2.class);

        PublishMessageCommandStep1.PublishMessageCommandStep3 publishMessageCommandStep3 = mock(
                PublishMessageCommandStep1.PublishMessageCommandStep3.class);

        ZeebeFuture<PublishMessageResponse> zeebeFutureMock = mock(ZeebeFuture.class);
        PublishMessageCommandStep1 publishMessageCommand = mock(PublishMessageCommandStep1.class);

        when(zeebeClient.newPublishMessageCommand()).thenReturn(publishMessageCommand);
        when(publishMessageCommand.messageName(anyString())).thenReturn(publishMessageCommandStep2);
        when(publishMessageCommandStep2.correlationKey(anyString())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.timeToLive(any())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.variables(anyMap())).thenReturn(publishMessageCommandStep3);
        when(publishMessageCommandStep3.send()).thenReturn(zeebeFutureMock);

        PayBillRouteProcessor.workflowInstanceStore.put(requestDto.getOafValidationRef(), "TEST-INSTANCE-123");
        // Act
        processor.processRequestForPayBillPayRoute(exchange);

        // Assert
        verify(zeebeClient).newPublishMessageCommand();
    }

    @DisplayName("Validate transaction ID that does not exist in the system")
    @Test
    void test_validate_non_existing_transaction_id() {
        // Arrange
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);

        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn(null);
        String transactionId = "test-123";

        // Act & Assert
        Assertions.assertDoesNotThrow(() -> processor.validateUniqueTransactionId(transactionId));
    }

    @DisplayName("Handle JsonProcessingException during response deserialization")
    @Test
    void test_handle_json_processing_exception() {
        // Arrange
        String invalidJson = "invalid-json";
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);

        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn(invalidJson);
        String transactionId = "test-123";

        // Act & Assert
        Assertions.assertThrows(JsonProcessingException.class, () -> processor.validateUniqueTransactionId(transactionId));
    }

    // Process response with COMMITTED transfer state throws exception
    @Test
    void test_committed_transfer_state() {
        String validJson = "{\"transferState\":\"COMMITTED\"}";
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);

        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn(validJson);
        String transactionId = "test-123";

        // Act & Assert
        Assertions.assertThrows(TnmConnectorExistingTransactionIdException.class,
                () -> processor.validateUniqueTransactionId(transactionId));
    }

    @DisplayName("Process response with non-COMMITTED transfer state")
    @Test
    void test_non_committed_transfer_state() {
        String validJson = "{\"transferState\":\"RECEIVED\"}";
        Exchange mockExchange = mock(Exchange.class);
        Message mockMessage = mock(Message.class);

        when(producerTemplate.send(eq("direct:paybill-transaction-status-check-base"), any(Processor.class))).thenReturn(mockExchange);
        when(mockExchange.getIn()).thenReturn(mockMessage);
        when(mockMessage.getBody(String.class)).thenReturn(validJson);
        String transactionId = "test-123";

        // Act & Assert
        Assertions.assertDoesNotThrow(() -> processor.validateUniqueTransactionId(transactionId));
    }

    @DisplayName("Valid transaction ID in header leads to successful processing")
    @Test
    void test_valid_transaction_id_processing() {

        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        Message message = exchange.getIn();
        message.setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, "valid-transaction-id");

        processor.processRequestForTransactionStatusCheck(exchange);

        Assertions.assertEquals("application/json", exchange.getIn().getHeader(CONTENT_TYPE));
        Assertions.assertEquals("transfers", exchange.getIn().getHeader("requestType"));
        Assertions.assertEquals("malawi", exchange.getIn().getHeader(TENANT_ID));
        Assertions.assertEquals("valid-transaction-id", exchange.getProperty(PAYBILL_TRANSACTION_ID_URL_PARAM));
    }

    @DisplayName("Null transaction ID in header throws MissingFieldException")
    @Test
    void test_null_transaction_id_throws_exception() {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        Message message = exchange.getIn();
        message.setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, null);

        Assertions.assertThrows(MissingFieldException.class, () -> {
            processor.processRequestForTransactionStatusCheck(exchange);
        });
    }

    @DisplayName("Null transaction ID in header throws MissingFieldException")
    @Test
    void test_empty_transaction_id_throws_exception() {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        Message message = exchange.getIn();
        message.setHeader(PAYBILL_TRANSACTION_ID_URL_PARAM, "");

        Assertions.assertThrows(MissingFieldException.class, () -> {
            processor.processRequestForTransactionStatusCheck(exchange);
        });
    }

    @DisplayName("Successfully processes PayBill validation response with valid channel response and client correlation ID")
    @Test
    void test_successful_payBill_validation_response_processing() {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();

        String channelResponse = "{\"transactionId\":\"123\"}";
        exchange.getIn().setBody(channelResponse);
        exchange.getIn().setHeader(X_CORRELATION_ID, "corr-123");
        exchange.getIn().setHeader(CLIENT_NAME, "John Doe");

        PayBillRouteProcessor.reconciledStore.put("corr-123", true);

        processor.processResponseForPayBillValidationResponseSuccess(exchange);

        String responseBody = exchange.getIn().getBody(String.class);
        JSONObject response = new JSONObject(responseBody);

        Assertions.assertEquals(200, response.getInt("status"));
        Assertions.assertEquals("Account exists", response.getString("message"));
        Assertions.assertEquals("corr-123", response.getString("oafTransactionReference"));
        Assertions.assertEquals("John Doe", response.getString("clientName"));
        Assertions.assertEquals("123", PayBillRouteProcessor.workflowInstanceStore.get("corr-123"));
        Assertions.assertFalse(PayBillRouteProcessor.reconciledStore.containsKey("corr-123"));
    }

    @DisplayName("Processes error response")
    @Test
    void test_process_error_response_creates_validation_response() {
        Exchange exchange = camelContext.getEndpoint("mock:test").createExchange();
        JSONObject errorResponse = new JSONObject();
        errorResponse.put("error", "Some error");
        exchange.getIn().setBody(errorResponse.toString());

        processor.processResponseForPayBillValidationResponseError(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        Assertions.assertEquals(404, response.getInt("status"));
        Assertions.assertEquals("Account does not exists or payment not allowed", response.getString("message"));
        Assertions.assertNull(response.optString("clientName", null));
        Assertions.assertNull(response.optString("oafTransactionReference", null));
    }

}
