package au.com.gritmed.pay;

import lombok.Data;

import java.util.Map;

@Data
public class Payment {
    private CharSequence id;
    private CharSequence workflow;
    private CharSequence workflowStep;
    private Map<CharSequence, CharSequence> securityData;
    private CharSequence payload;
    private Map<CharSequence, CharSequence> history;
    private CharSequence status;
}
