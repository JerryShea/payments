package au.com.gritmed.pay;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

public class PaymentProducerTest {
    private static final String DOMESTIC = "domestic-v1";
    private static final String LIMIT_CHECK = "limit-check";
    private static final String IN_PROGRESS = "in-progress";

    @Test
    public void producingPayment() throws Exception {
        var paymentAppender = new PaymentAppender();
        IntStream.range(0, 100000).forEach(i -> paymentAppender.append(createPayment()));
    }

    private Payment createPayment() {
        var payment = new Payment();
        payment.setId(UUID.randomUUID().toString());
        payment.setWorkflow(DOMESTIC);
        payment.setWorkflowStep(LIMIT_CHECK);
        payment.setSecurityData(securityData());
        payment.setPayload(RandomStringUtils.randomAlphanumeric(3500));
        payment.setHistory(history());
        payment.setStatus(IN_PROGRESS);

        return payment;
    }

    private Map<CharSequence, CharSequence> securityData() {
        return Map.of(
                "****", "**********",
                "***********************", "**********",
                "***", "************************************"
        );
    }

    private Map<CharSequence, CharSequence> history() {
        return Map.of(
                "*******", RandomStringUtils.randomAlphanumeric(256),
                "****************", RandomStringUtils.randomAlphanumeric(3500)
        );
    }
}
