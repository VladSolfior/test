###### Sample service module processing message from message broker.

in plans: adding DynamoDb as dao implementation, kafka broker

 **Process message by**:
  - pulling it from broker,
  - ordering 
  - sending to another topic
  - acknowledging / throwing an exception.
  
  Added some sample testing cases.