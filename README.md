# java-azureservicebus-samples

---
#####  1. AzureServiceBusSendReceive
---
The sample code  demonstrates ways to create the queue or topic in the azure service bus namespace and process the message.

##### Dependencies

```
	<!-- JMS -->
	<dependencies>
		<dependency>
    		<groupId>com.azure</groupId>
    		<artifactId>azure-messaging-servicebus</artifactId>
    		<version>7.7.0</version>
		</dependency>
	</dependencies>
```

Use ServiceBusAdministrationClient to create the queue or topic.

```
ServiceBusAdministrationClient client = new ServiceBusAdministrationClientBuilder()
				     .connectionString(connectionString)
				     .buildClient();
```

With the client, queue/topic can be created using ;

- client.createQueue(queueName) ;
- client.createTopic(topicName) ;
  client.createSubscription(topicName, subName);
  
If a queue exists with the same queueName., this will throw ResourceExistsException. Handle the exception or alternatively use the listQueue method to check if the queue already exists and create only when required.

Also Throws:

	* ClientAuthenticationException - if the client's credentials do not have access to modify the namespace.
	* HttpResponseException - If the request body was invalid, the queue quota is exceeded, or an error occurred processing the request.
	* NullPointerException - if queueName is null.
	* IllegalArgumentException - if queueName is an empty string.
	

---
##### 2. AzureServiceBusDLQ
---

Azure Service Bus queues and topic subscriptions provide a secondary subqueue, called a dead-letter queue (DLQ). The dead-letter queue doesn't need to be explicitly created and can't be deleted or otherwise managed independent of the main entity.

Reference: https://azuresdkdocs.blob.core.windows.net/$web/java/azure-messaging-servicebus/7.0.0-beta.7/com/azure/messaging/servicebus/models/SubQueue.html


###### Submit message with low TTL to send to DLQ
![image](https://user-images.githubusercontent.com/85903942/207406898-0e2e477c-a907-4e86-8dae-8737c7c91261.png)


###### Processing DLQ messages: 

```
Process DLQ message
Starting the DLQ processor
Processing message. Session: 77084e6699234b9491f6eb5e54aa8862, Sequence #: 27. Contents: Message 1
Processing message. Session: 4ddec4bf5bba4495a213d0d3bbe1cf58, Sequence #: 28. Contents: Message 2 dup
Stopping and closing the DLQ processor
Completed processing messages
```

Alternate Option: https://github.com/Azure/azure-service-bus/blob/master/samples/Java/azure-servicebus/DeadletterQueue/src/main/java/com/microsoft/azure/servicebus/samples/deadletterqueue/DeadletterQueue.java


	
