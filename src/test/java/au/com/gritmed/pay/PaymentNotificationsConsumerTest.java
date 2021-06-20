package au.com.gritmed.pay;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.Pauser;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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

    private final Pauser pauser = new LongPauser(0, 0, 1, 20_000, TimeUnit.MICROSECONDS);;
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

        long lastTime = System.currentTimeMillis();
        do {
            paymentIndex = notificationTailer.readText();
            if (Objects.nonNull(paymentIndex)) {
                payment = readPayment(Long.parseLong(paymentIndex));
                assertNotNull(payment, "Unable to retrieve payment for index " + paymentIndex);
                if (++counter % 10000 == 0) {
                    System.out.println(LocalDateTime.now() + ": Consumed " + counter + " payments");
                }
                if (counter % 100000 == 0) {
                    showPause(lastTime);
                    lastTime = System.currentTimeMillis();
                }
            }
        } while (true);

    }

    private void showPause(long lastTime) {
        long now = System.currentTimeMillis();
        long timeDelta = now - lastTime;
        long timePaused = pauser.timePaused();
        long countPaused = pauser.countPaused();
        if (countPaused == 0)
            System.out.println("no pauses");
        else {
            double averageTime = timePaused * 1000 / countPaused / 1e3;
            double busy = Math.abs((timeDelta - timePaused) * 1000 / timeDelta / 10.0);
            System.out.println("pauser avg pause: " + averageTime + " ms, "
                    + "count=" + countPaused
                    + (lastTime > 0 ? ", busy=" + busy + "%" : ""));
        }
    }

    private Payment readPayment(long paymentIndex) throws IOException {
        pauser.reset();
        while (!paymentsTailer.moveToIndex(paymentIndex))
            pauser.pause();
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
