package com.messaging;

import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.QueueProperties;
import com.azure.messaging.servicebus.administration.models.SubscriptionProperties;
import com.azure.messaging.servicebus.administration.models.TopicProperties;
import com.azure.messaging.servicebus.models.SubQueue;

import io.netty.handler.ssl.ClientAuth;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.List;

//spring.jms.servicebus.connection-string: Endpoint=sb://txndemo.servicebus.windows.net/;SharedAccessKeyName=full;SharedAccessKey=i2+hWt8VS/BgdW+oND8yuLxAb7fAzCgFZnUs4gORj34=;EntityPath=primaryq
//spring.jms.servicebus.idle-timeout: 1800000
//spring.jms.servicebus.pricing-tier: Premium

public class AzureServiceBusSendReceive {
	static String connectionString = "Endpoint=sb://txndemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=i3Fz6ouLrsBayN9vCqzhzdujPJF12Aul6aRpE+UfTf8=";
	static String topicName = "demotopic";    
	static String queueName = "demoq";    
	static String subName = "demosub";
	
	public static void main(String[] args) throws InterruptedException {
		createQueueNTopic();
		
		System.out.println("Process DLQ message");
		receiveDLQMessages();
		
		
		sendMessage(true);		
		sendMessage(false);
		sendMessageBatch();
		receiveMessages(true);
		receiveMessages(false);
		System.out.println("Completed processing messages");
		System.exit(0);
	}
	
	static void createQueueNTopic()
	{
		ServiceBusAdministrationClient client = new ServiceBusAdministrationClientBuilder()
				     .connectionString(connectionString)
				     .buildClient();
		
		boolean status = client.listQueues().stream().anyMatch(q -> q.getName().equals(queueName));
		QueueProperties qp;
		if (!status) {
			qp = client.createQueue(queueName) ;
			System.out.println("Created :" + qp.getName());
		}
		
		status = client.listTopics().stream().anyMatch(t -> t.getName().equals(topicName));
		TopicProperties tp;
		if (!status) {
			tp =  client.createTopic(topicName);
			System.out.println("Created :" + tp.getName());
			
			SubscriptionProperties sp = client.createSubscription(topicName, subName);
			System.out.println("Created :" + sp.getSubscriptionName());
		}	
		
		
		   
	}
	
	static void sendMessage(boolean isQueue)
	{
		ServiceBusSenderClient senderClient ;

		if (isQueue) {
			senderClient = new ServiceBusClientBuilder()
		            .connectionString(connectionString)
		            .sender()
		            .queueName(queueName)
		            .buildClient();
		}
		else {
	    // create a Service Bus Sender client for the queue 
			senderClient = new ServiceBusClientBuilder()
	            .connectionString(connectionString)
	            .sender()
	            .topicName(topicName)
	            .buildClient();
		}
	    // send one message to the topic
	    senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
	    System.out.println("Sent a single message to the topic: " + topicName);        
	}
	
	static List<ServiceBusMessage> createMessages()
	{
	    // create a list of messages and return it to the caller
	    ServiceBusMessage[] messages = {
	    		new ServiceBusMessage("First message"),
	    		new ServiceBusMessage("Second message"),
	    		new ServiceBusMessage("Third message")
	    };
	    return Arrays.asList(messages);
	}
	
	static void sendMessageBatch()
	{
	    // create a Service Bus Sender client for the topic 
	    ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .connectionString(connectionString)
	            .sender()
	            .topicName(topicName)
	            .buildClient();

	    // Creates an ServiceBusMessageBatch where the ServiceBus.
	    ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();        

		// create a list of messages
	    List<ServiceBusMessage> listOfMessages = createMessages();

	    // We try to add as many messages as a batch can fit based on the maximum size and send to Service Bus when
	    // the batch can hold no more messages. Create a new batch for next set of messages and repeat until all
	    // messages are sent.        
	    for (ServiceBusMessage message : listOfMessages) {
	        if (messageBatch.tryAddMessage(message)) {
	            continue;
	        }

	        // The batch is full, so we create a new batch and send the batch.
	        senderClient.sendMessages(messageBatch);
	        System.out.println("Sent a batch of messages to the topic: " + topicName);

	        // create a new batch
	        messageBatch = senderClient.createMessageBatch();

	        // Add that message that we couldn't before.
	        if (!messageBatch.tryAddMessage(message)) {
	            System.err.printf("Message is too large for an empty batch. Skipping. Max size: %s.", messageBatch.getMaxSizeInBytes());
	        }
	    }

	    if (messageBatch.getCount() > 0) {
	        senderClient.sendMessages(messageBatch);
	        System.out.println("Sent a batch of messages to the topic: " + topicName);
	    }

	    //close the client
	    senderClient.close();
	}
	
	// handles received messages
	static void receiveMessages(boolean isQueue) throws InterruptedException
	{
	    CountDownLatch countdownLatch = new CountDownLatch(1);
	    
	    ServiceBusProcessorClient processorClient;
	    
	    if(isQueue) {
	    	   // Create an instance of the processor through the ServiceBusClientBuilder
		     processorClient = new ServiceBusClientBuilder()
		        .connectionString(connectionString)
		        .processor()
		        .queueName(queueName)
		        .processMessage(AzureServiceBusSendReceive::processMessage)
		        .processError(context -> processError(context, countdownLatch))
		        .buildProcessorClient();
	    }
	    else {
	    	   // Create an instance of the processor through the ServiceBusClientBuilder
		     processorClient = new ServiceBusClientBuilder()
		        .connectionString(connectionString)
		        .processor()
		        .topicName(topicName)
		        .subscriptionName(subName)
		        .processMessage(AzureServiceBusSendReceive::processMessage)
		        .processError(context -> processError(context, countdownLatch))
		        .buildProcessorClient();
	    }
	 

	    System.out.println("Starting the processor");
	    processorClient.start();

	    TimeUnit.SECONDS.sleep(3);
	    System.out.println("Stopping and closing the processor");
	    processorClient.close();    	
	}
	
	static void receiveDLQMessages() throws InterruptedException
	{
	    CountDownLatch countdownLatch = new CountDownLatch(1);
	    ServiceBusAdministrationClient client = new ServiceBusAdministrationClientBuilder()
			     .connectionString(connectionString)
			     .buildClient();
	    
	    ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
		        .connectionString(connectionString)
		        .processor()
		        .queueName(queueName).subQueue(SubQueue.DEAD_LETTER_QUEUE)
		        .processMessage(AzureServiceBusSendReceive::processMessage)
		        .processError(context -> processError(context, countdownLatch))
		        .buildProcessorClient();
		        
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
	        System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
	            reason, exception.getMessage());

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
	        System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(),
	            reason, context.getException());
	    }
	}
	

}


