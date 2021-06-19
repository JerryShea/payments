package au.com.gritmed.pay;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

import static au.com.gritmed.pay.PaymentAppender.LIMIT_CHECK;
import static au.com.gritmed.pay.PaymentAppender.PAYMENTS;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PaymentNotificationsConsumerTest {
    private static final String ID = "id";
    private static final String WORKFLOW = "workflow";
    private static final String WORKFLOW_STEP = "workflowStep";
    private static final String SECURITY_DATA = "securityData";
    private static final String PAYLOAD = "payload";
    private static final String HISTORY = "history";
    private static final String STATUS = "status";

    private ExcerptTailer paymentsTailer;
    private ExcerptTailer notificationTailer;
    private AvroHelper avroHelper;

    @BeforeEach
    void setUp() throws IOException {
        paymentsTailer = SingleChronicleQueueBuilder.binary(PAYMENTS).build().createTailer("payment-consumer");
        notificationTailer = ChronicleQueue.single(LIMIT_CHECK).createTailer("limit-check-consumer");
        avroHelper = new AvroHelper();
    }

    @Test
    public void consumingPaymentNotifications() throws Exception {
        String paymentIndex;
        Payment payment;
        var counter = 0;

        do {
            paymentIndex = notificationTailer.readText();
            if (Objects.nonNull(paymentIndex)) {
                payment = readPayment(Long.parseLong(paymentIndex));
                assertNotNull(payment, "Unable to retrieve payment for index " + paymentIndex);
                if (++counter % 10000 == 0) {
                    System.out.println(LocalDateTime.now() + ": Consumed " + counter + " payments");
                }
            }
        } while (true);

    }

    private Payment readPayment(long paymentIndex) throws IOException {
        paymentsTailer.moveToIndex(paymentIndex);
        try (var documentContext = paymentsTailer.readingDocument()) {
            if (Objects.isNull(documentContext.wire())) {
                return null;
            } else {
                var paymentRecord = avroHelper.readFromInputStream(documentContext.wire().bytes().inputStream());
                return createPayment(paymentRecord);
            }
        }
    }

    private Payment createPayment(GenericRecord paymentRecord) {
        var payment = new Payment();
        payment.setId((CharSequence) paymentRecord.get(ID));
        payment.setWorkflow((CharSequence) paymentRecord.get(WORKFLOW));
        payment.setWorkflowStep((CharSequence) paymentRecord.get(WORKFLOW_STEP));
        payment.setSecurityData((Map<CharSequence, CharSequence>) paymentRecord.get(SECURITY_DATA));
        payment.setPayload((CharSequence) paymentRecord.get(PAYLOAD));
        payment.setHistory((Map<CharSequence, CharSequence>) paymentRecord.get(HISTORY));
        payment.setStatus((CharSequence) paymentRecord.get(STATUS));

        return payment;
    }
}
