# payments
To recreate the issue first run PaymentNotificationsConsumerTest
That will stay in a loop forever consuming notifications arriving in /tmp/payments/limit-check chronicle queue
Each notification consumed from that queue is in fact the text representation of a payment added in the /tmp/payments queue
If unable to find the payment based on payment index notification the test will fail with an assertion error showing the index not found


While the above test is running run PaymentProducerTest
That will write 100,000 payments to the /tmp/payments queue and send a payment index notification to the /tmp/payments/limit-check queue
Repeat the run or increase the number of payments written if unable to reproduce.
For me it happens for every single run on both my Macbook work machine and my linux Fedora personal machine

