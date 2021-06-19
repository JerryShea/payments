package au.com.gritmed.pay;

import lombok.SneakyThrows;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.io.IOException;

public class PaymentAppender {
    static final String PAYMENTS = "/tmp/payments";
    static final String LIMIT_CHECK = PAYMENTS + "/limit-check";

    private final AvroHelper avroHelper;
    private final ChronicleQueue paymentsQueue;
    private final ChronicleQueue limitCheckQueue;

    public PaymentAppender() throws IOException {
        avroHelper = new AvroHelper();
        paymentsQueue = SingleChronicleQueueBuilder.binary(PAYMENTS).build();
        limitCheckQueue = ChronicleQueue.single(LIMIT_CHECK);
    }

    @SneakyThrows
    public String append(Payment payment) {
        var paymentRecord = avroHelper.getGenericRecord();

        paymentRecord.put("id", payment.getId());
        paymentRecord.put("workflow", payment.getWorkflow());
        paymentRecord.put("workflowStep", payment.getWorkflowStep());
        paymentRecord.put("securityData", payment.getSecurityData());
        paymentRecord.put("payload", payment.getPayload());
        paymentRecord.put("history", payment.getHistory());
        paymentRecord.put("status", payment.getStatus());

        try (var documentContext = paymentsQueue.acquireAppender().writingDocument()) {
            avroHelper.writeToOutputStream(paymentRecord, documentContext.wire().bytes().outputStream());
            var paymentIndex = documentContext.index();

            return String.valueOf(paymentIndex) + '/' + notifyLimitCheck(paymentIndex);
        }
    }

    private long notifyLimitCheck(long paymentIndex) {
        var appender = limitCheckQueue.acquireAppender();
        appender.writeText(String.valueOf(paymentIndex));
        return appender.lastIndexAppended();
    }
}
