Here is a conversation between Rahul and Amey:

*September 17th*
**Rahul Jain:**
Hi Mike Baranski Can you please confirm if there are any APIs we built for Pfizer that allows us to see what facts are involved for a particular exception. Currently all the facts are templatized in an exception and shown as a text message inside the Exception Description text box.  

**Mike Baranski:**
I can look tonight.  I may need to ask Anup or Justin Mader tomorrow.

*September 18th*
**Mike Baranski:**
We can query exceptions by date or order #, but we don’t parse the text in the comments/descriptions into separate facts.

**Rahul Jain:**
Okay is there any table being maintained that contains these facts or these are only present in exception description ? 

**Mike Baranski:**
Only in the description. PharmaSuite was designed to provide information for the batch report, and not for detailed analytics.  I think that’s why it’s of value if you separate the free-formed text into any number of facts using AI, then assign them as separate pieces of context to that exception.

*September 23th*
**Rahul Jain:**
Amey Patil We need access to Dev environment of PS, ActiveMQ so that we can test configurations. Can you check on it.  

*September 24th*
**Amey Patil:**
Rahul Jain
Following are the details for one of our internal Dev environment 
PS - http://aznaampstalos1d.ra-int.com:8080/PlantOperations/
Active MQ - http://aznaampstalos1d.ra-int.com:8162/
     Credentials - admin/admin
I've enabled the config for Exception Handling on this system. And the queues are available.
Do you want us to create a user for your testing in the PS system?

**Rahul Jain:**
Does every every single exception and comment come here as a separate XML or is this something that can be configured ?
 
*Nain Sandoval* Please work with Amey Patil to set this up and have it ready for development. Setup time with me tomorrow to showcase this. I want to see if in PEC as soon as a exception is generated there should be an entry in Active MQ, any subsequent comment in that exception should also generate one entry in ActiveMQ  
 
**Amey Patil:**
Every exception generates as a separate exception payload. It's not configurable since every exception has a separate instance.
 
**Rahul Jain:**
each exception can have a description and multiple comments. Does these come seperately ?
 
**Amey Patil:**
Comment as in operator comment on PEC?
 
**Rahul Jain:**
Yes

**Amey Patil:**
That is dependent on the Risk class

*September 25th*
**Rahul Jain:**
Hi Mike - Is there any api exposed which gives us access to exception data, namely below columns
Order Number / Workflow Number
Procedure Name
UP Name
OP Name
Phase Identifier
Exception Identifier (Exception Process Parameter Identifier)
Exception Description
Comments (Operator and/or Supervisor Comments)
Relevant Timestamp(s)
Signed By (Operator)
Supervisor Signed By (for double-signature , where applicable)
Exception Category (System Triggered / User Triggered)
 
**Mike Baranski:**
Let me see if I can open it up to see, or find the API documentation for you.  Jonathan Evans in LCS works full-time on Pfizer and knows these really well. 
 
You can review this design spec and see that there are SQL queries built that contain a lot of exception information, but whether they are also exposed in the REST API is to be determined.
DS-SQL-104301_PPRS.docx
 
The MESPublishingService spec also has a dedicated section on Exception data.  But this is still unclear if retrievable via REST API.
MESPublishingService.docx
 
**Rahul Jain:**
This is helpful, do you know the point of contact who can help us access this ? I have couple of questions on this
Is MESPublishingService switched on in any dev or test environment? Which one, and what version is it?
What are the broker URL and the queue and topic names (PublishingEventQueue, PublishingPublishTopic)?
Who can give us a test login for that broker ?
Can you send one real sample response for an order that has exceptions and comments?
Can you send a BatchRecord request for one dev order and share what comes back?

**Mike Baranski:**
Jonathan Evans

*September 29th*
**Rahul Jain:**
Amey Patil Are these accessible with rockwell vpn or pfizer ? 
yes please create a user for us but give us admin access

*September 29th*
**Rahul Jain:**
Nain Sandoval Once Thomas Swigon Confirms can you please work closely with Johnathan Evans and Amey on below items
Setup dev Pharma Suite and Active MQ in Pfizer environment
Write code to subscribe to Active MQ queue and push messages into SQS
Understand how to change the configuration in Pharma Suite risk level setting  "ActivateExternalExceptionHandling", "MinimalRiskClassForExternalExceptionHandling"
Write code to push messages back into the queue (which queue ?) for messages to be visible in PRC
Thomas Swigon Do we have a timeline on when Johnathan will be available, all open items currently has dependency on him. 
 
Write now there is one dev instance of PS and Active MQ available (Rockwell instance)
PS - http://aznaampstalos1d.ra-int.com:8080/PlantOperations/
Active MQ - http://aznaampstalos1d.ra-int.com:8162/
     Credentials - admin/admin
 
You should start figuring out ActiveMQ code to pick messages from this queue to save time. 


**Rahul Jain:**
Amey Patil Are these accessible with rockwell vpn or pfizer ? 
 
**Amey Patil:**
Rahul Jain This is accessible with rockwell network. This is our AMPS project internal development environment for one of the AMPS version having PS 10.2 version.

Rahul Jain I've created 2 users in PS - http://aznaampstalos1d.ra-int.com:8080/PlantOperations/ 
User 1 Rahul Jain - Credentials RJ/RJ
User 2 Nain Sandoval - Credentials NS/NS