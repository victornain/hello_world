**What's confirmed, straight from the source**
 
**The exact message PharmaSuite sends when an exception fires** (`ProcessRecordedException`, over ActiveMQ/JMS) carries: exception ID, order/workflow ID, a free-text description, risk, category, and classification (all as structured fields), signatures, and — importantly — `ExceptionReference`, a structured Unit-Procedure/Operation/Phase path. This looks like the real phase identifier we've been reconstructing by hand from `UP_OP_PH`.
 
**The exact message we'd send back** (`ConfirmRecordedException`) travels over a specifically named queue — `EXCEPTION_RECORD_EXTERNAL_RESPONSE` — and supports setting the exception's status and appending our comment. This is a complete, documented write-back schema, not something we need to design ourselves.
 
**Connection details are documented:** standard broker URL `ssl://<host>:61647`, schema files `RAExternalExceptionHandling.xsd` in `ftps-b2mml-v0600-<version>.jar`.
 
**Confirmation:** the "classification" field Jeremiah showed in the demo (e.g. "human error") is real and structured — `ExceptionClassificationChoiceElement` — not something we misread.
 
**What's missing from the schema itself**
The message carries exception details and metadata — **not** the underlying process parameters (measured value, limit violated, etc.) as separate fields. If that's not already worked into the free-text description, it doesn't come through this path at all.
 
**The open item — needs config team specifically**
**By default, this mechanism only sends High-risk exceptions.** The doc states the risk-class threshold is configurable (`MinimalRiskClassForExternalExceptionHandling`), but High is the out-of-the-box default. **We need to confirm whether Pfizer has actually lowered this** — because as designed out of the box, this integration would only give us the highest-risk exceptions, not the full Tier A/B population the whole project is meant to help with. This is the one finding that could change our approach if the answer comes back wrong, so it should be first on the list.

Question solved by Justin :
1. **Is ActivateExternalExceptionHandling actually turned on for our three sites, and at what risk threshold?** Currently not enabled at any Pfizer site so customer would need to adjust their deployments
2. **Does ExceptionReference in the message really correspond to the phase identifier or Exception Identifier ? Or which XML field gives us this?** This the path or reference to the exception, PSUniqueExceptionID is the exception identifier in PharmaSuite
3. **What's the actual outbound queue name and connection details for our specific PharmaSuite environment? (The doc names the response queue explicitly; the outbound one wasn't as clearly named in what we read.)** See above diagram, there is some missing information in the PS Technical Manual.
4. **Is ActivateExceptionClassification turned on, and if so, what does Pfizer's classification list actually contain?** I do not believe Pfizer uses exception classification
5. **Could the getOtherInformation() extension point be used to add richer process-parameter data to the message, if we ever need it?** Yes

**Other questions:**
- **Q:** do they have multiple instances of PharmaSuite and if so, can we configure each site to submit to the same queue?
- **A:** They have multiple instances of Pharmasuite, each site has their own, and each site has their own EiHub, Tomcat,  or Mule instance, depending on which version of Pharmasuite. They can not send to the same queue.
- **Q:** Can you please confirm if there are any APIs we built for Pfizer that allows us to see what facts are involved for a particular exception? Currently all the facts are templatized in an exception and shown as a text message inside the Exception Description text box.
- **R:** We can query exceptions by date or order #, but we don’t parse the text in the comments/descriptions into separate facts.
- **Q:** is there any table being maintained that contains these facts or these are only present in exception description?
- **R:** Only in the description. PharmaSuite was designed to provide information for the batch report, and not for detailed analytics.  I think that’s why it’s of value if you separate the free-formed text into any number of facts using AI, then assign them as separate pieces of context to that exception.

**New Information from Amey**
1.	**What is the exact broker URL/host and port for each of the 3 sites (Andover, Grange Castle DP, Sanford) individually?**
•	 This can be answered/guided by the Pfizer Core (Eric Smith) and Solution Engineering teams.
2.	**What credentials or certificates (TLS keystore/truststore, service account) do we need for the connector to authenticate against each site's broker?**
•	 This can be answered/guided by the Pfizer Core (Eric Smith) and Solution Engineering teams.
3.	**Can you confirm the exact name of the inbound queue (PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST) and the outbound queue (EXCEPTION_RECORD_EXTERNAL_RESPONSE) per site and per environment (dev/test/prod), since names may not be identical across sites?**
•	This will be the same queue names since they are provided by the standard RA product team and there is no customization.
4.	**Which integration layer (EiHub, Tomcat, or Mule) is deployed at each site, and does that change anything about how we connect or the message format we should expect?**
•	EiHub / EIG is the integration layer is deployed, and the connection and the message format would remain the same.
5.	**Is there already a staging/test ActiveMQ environment (for at least one site) where we can validate the connector before production?**
•	No, we don't have any internal ActiveMQ environment for these sites at the moment. You can use the internal environment provided by Justin the other for validating the connector tcp://aznaampstalos1i:61646 PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST & tcp://aznaampstalos1i:61646 EXCEPTION_RECORD_EXTERNAL_RESPONSE
6.	**What network/firewall access is required for our on-prem connector to reach the broker from its current location?**
•	tcp ports should be kept open to access / communicate with 61646 connector port.
7.	**Does the broker already have retry/DLQ policies configured on the ActiveMQ side, or is that entirely the responsibility on the AWS side?**
•	There is Time to live (TTL) on these messages and since in the PhramaSuite AMQ instance are not persisted. Our team is supporting use of the standard PS integration scenario (current scope).  Whatever consumes from this queue should persist the messages as needed.
8.	**What is the expected message volume/throughput per site, so we can size the connector and consumer accordingly?**
•	This can be answered/guided by the Pfizer Core (Eric Smith) and Solution Engineering teams.

