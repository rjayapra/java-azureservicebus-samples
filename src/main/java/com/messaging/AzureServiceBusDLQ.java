package com.messaging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.models.SubQueue;

public class AzureServiceBusDLQ {

	// spring.jms.servicebus.connection-string:
	// Endpoint=sb://txndemo.servicebus.windows.net/;SharedAccessKeyName=full;SharedAccessKey=i2+hWt8VS/BgdW+oND8yuLxAb7fAzCgFZnUs4gORj34=;EntityPath=primaryq
	// spring.jms.servicebus.idle-timeout: 1800000
	// spring.jms.servicebus.pricing-tier: Premium

	static String connectionString = "Endpoint=sb://txndemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=i3Fz6ouLrsBayN9vCqzhzdujPJF12Aul6aRpE+UfTf8=";
	static String queueName = "primaryq";

	public static void main(String[] args) throws InterruptedException {
		System.out.println("Process DLQ message");
		receiveDLQMessages();

		System.out.println("Completed processing messages");
		System.exit(0);
	}

	static void receiveDLQMessages() throws InterruptedException {
		CountDownLatch countdownLatch = new CountDownLatch(1);
		ServiceBusAdministrationClient client = new ServiceBusAdministrationClientBuilder()
				.connectionString(connectionString).buildClient();

		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder().connectionString(connectionString)
				.processor().queueName(queueName).subQueue(SubQueue.DEAD_LETTER_QUEUE)
				.processMessage(AzureServiceBusDLQ::processMessage)
				.processError(context -> processError(context, countdownLatch)).buildProcessorClient();

		System.out.println("Starting the DLQ processor");
		processorClient.start();

		TimeUnit.SECONDS.sleep(3);
		System.out.println("Stopping and closing the DLQ processor");
		processorClient.close();
	}

	private static void processMessage(ServiceBusReceivedMessageContext context) {
		ServiceBusReceivedMessage message = context.getMessage();
		System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", message.getMessageId(),
				message.getSequenceNumber(), message.getBody());
	}

	private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
		System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
				context.getFullyQualifiedNamespace(), context.getEntityPath());

		if (!(context.getException() instanceof ServiceBusException)) {
			System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
			return;
		}

		ServiceBusException exception = (ServiceBusException) context.getException();
		ServiceBusFailureReason reason = exception.getReason();

		if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
				|| reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
				|| reason == ServiceBusFailureReason.UNAUTHORIZED) {
			System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n", reason,
					exception.getMessage());

			countdownLatch.countDown();
		} else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
			System.out.printf("Message lock lost for message: %s%n", context.getException());
		} else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
			try {
				// Choosing an arbitrary amount of time to wait until trying again.
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				System.err.println("Unable to sleep for period of time");
			}
		} else {
			System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(), reason,
					context.getException());
		}
	}

}
