# Work Plan: Ingesting PharmaSuite Exceptions (ActiveMQ) into SQL

- **Author:** Victor Nain Sandoval
- **Scope:** Get exception data out of PharmaSuite, publish/consume it via ActiveMQ, and store it in SQL.
- **Key collaborators:** Justin (PharmaSuite config), Amey (ActiveMQ owner), Jonathan Evans (MESPublishingService/API, pending Thomas Swigon's confirmation)
- **Last updated:** 2026-09-30 — Jonathan confirmed the MESPublishingService API is hosted on the **same box** as Amey's Rockwell ActiveMQ/PS instance (`aznaampstalos1d.ra-int.com`) — both integration paths can now be validated against one environment

## Objective

Build the **ingestion** stage of the flow: PharmaSuite (`ProcessRecordedException`) → ActiveMQ → on-prem connector → SQS `exception-in` → consumer → SQL storage, meeting the acceptance criteria of S1 (auto-ingestion with no manual step, resilience if the exception module is unavailable, idempotency check) and S2 (tagging by site and archetype).

## Key architecture facts confirmed by the stakeholder Q&A

- **No shared queue across sites.** Pfizer runs a separate PharmaSuite instance per site, each with its own EiHub/Tomcat/Mule integration layer (depending on the PharmaSuite version at that site). Sites **cannot** publish to a single shared ActiveMQ queue. This means the connector must be deployed/configured **per site**, each with its own broker connection, rather than one connector pointed at one shared endpoint.
- **Queue names and message format are standard across sites (confirmed by Amey).** The inbound queue (`PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST`) and outbound queue (`EXCEPTION_RECORD_EXTERNAL_RESPONSE`) names come from the standard RA product and are **not customized per site**. All sites run EiHub/EIG as the integration layer, so the connection and message format are the same everywhere. This means the connector's per-site configuration only needs to vary by **broker URL and credentials**, not by queue naming or message parsing logic.
- **No internal ActiveMQ staging environment for the 3 production sites.** Amey confirmed there is no site-specific staging/test broker. Justin has provided an internal validation environment instead: `tcp://aznaampstalos1i:61646` for both the inbound (`PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST`) and outbound (`EXCEPTION_RECORD_EXTERNAL_RESPONSE`) queues. Note: this uses **plain `tcp` on port 61646**, whereas the documented production connection is **`ssl` on port 61647**.
- **DLQ/retry responsibility is entirely on our (AWS) side.** Amey confirmed PharmaSuite's ActiveMQ messages are not persisted — they only have a TTL. The team supports the standard PS integration scenario only; whatever consumes from the queue (our connector/SQS pipeline) is responsible for persisting messages as needed. This confirms the Phase 3 DLQ/alerting design must not rely on any broker-side retry.
- **No structured "facts" table or API.** PharmaSuite does not expose an API or table with the individual facts (measured value, limit violated, etc.) behind an exception. All of that information is templatized as free text inside the exception's Description field, since PharmaSuite was built for batch-report purposes, not analytics. Any structured "facts" we want (beyond the fields already in `ProcessRecordedException`) will have to be derived later by parsing the free-text description with AI — this is not something the ingestion pipeline gets for free from the source system.
- **A second, separate dev instance exists ("Rockwell instance") that also hosts the MESPublishingService API (confirmed Sept 30).** Amey (Sept 24/29) gave access to `PS - http://aznaampstalos1d.ra-int.com:8080/PlantOperations/` and `ActiveMQ - http://aznaampstalos1d.ra-int.com:8162/` (admin/admin), with Exception Handling config already enabled and queues available. Amey described it as "our AMPS project internal development environment for one of the AMPS version having PS 10.2 version." Separately, Jonathan Evans confirmed the MESPublishingService REST API (the alternate/parallel integration path Rahul asked Mike about on Sept 25) **is hosted on this same box** ("Yes, the MES Pub service is hosted on that box"). This means **both integration paths — the ActiveMQ `ProcessRecordedException` flow and the MESPublishingService/API flow — can be validated against a single shared dev environment**, which simplifies Phase 1/2 discovery considerably. It's still a genuinely separate environment from Justin's ActiveMQ validation environment (`aznaampstalos1i`, `tcp://...:61646`) — that relationship remains a low-priority open item. **Access confirmed (Sept 30): reachable only via Rockwell network, not Pfizer VPN.** Note also that the URL given (`:8162`) is the ActiveMQ **web console** port, not a message-protocol port (OpenWire/AMQP) — the actual port needed to programmatically consume messages is still unknown for this instance, and the MESPublishingService broker URL/queue/topic names are still unknown too (see Jonathan questions below).
- **Personalized PS logins now exist for Rahul and Nain (Sept 30), but ActiveMQ access is still shared.** Amey created individual users in the PS web app (`PlantOperations`) — Rahul: `RJ/RJ`, Nain: `NS/NS`. This does **not** change the ActiveMQ side: the ActiveMQ web console at `:8162` is still shared `admin/admin`. Rahul's ask for "admin access" (Sept 29) hasn't been explicitly confirmed as granted at an admin permission level within PS — only that two named accounts now exist.
- **Browser/web-console access to both queues confirmed (Sept 30, Nain).** Can browse `PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST` and `EXCEPTION_RECORD_EXTERNAL_RESPONSE` directly in the ActiveMQ web console. This confirms queue existence/reachability, and unblocks self-serve testing of a few open questions below (Risk Class comment behavior, multiple-comment behavior, response queue payload shape) by generating test exceptions in PS and watching the queues — without needing to wait on Amey for those specific items. It does **not** provide a message-protocol connection (still just the HTTP web console), so it doesn't remove the need to get the OpenWire/AMQP/STOMP port for Phase 2's actual connector code.
- **Whether the operator/supervisor comment arrives as its own message depends on Risk Class (confirmed by Amey, Sept 24).** Amey confirmed this is conditional, not uniform. Sample `ProcessRecordedException` payloads we already have (`ProcessRecordedException.xml`, `Outbound messages/RecordedException1.xml`) show the comment embedded as free text *inside* `ExceptionDescription` (e.g. "Comment: This comment from DCS...") rather than in a distinct field — so the ingestion/parsing logic cannot assume one fixed shape and must handle both cases once Risk Class thresholds are confirmed.
- **The MESPublishingService REST API is confirmed live on the Rockwell dev box, not just "unruled out."** Rahul asked Mike (Sept 25) about a REST API exposing exception columns (Procedure Name, UP Name, OP Name, Phase Identifier, comments, signatures, etc.) via `PublishingEventQueue`/`PublishingPublishTopic`. Jonathan Evans confirmed (Sept 30) this service is hosted on `aznaampstalos1d.ra-int.com` — the same box as the ActiveMQ dev instance (see fact above). It's still unconfirmed whether/how this API path relates to (replaces, complements, or is fully independent of) the `ProcessRecordedException`/ActiveMQ flow this plan is built around — that determination still needs the broker URL, queue/topic names, and a sample response from Jonathan (see questions below). If confirmed as a source for the extra columns, it could affect the field mapping in Phase 0.4.

## Phase 0 — Configuration prerequisites (with Justin)

1. Confirm and request activation of `ActivateExternalExceptionHandling` at all 3 sites (Andover, Grange Castle DP, Sanford) — currently disabled everywhere.
2. Agree on and confirm the `MinimalRiskClassForExternalExceptionHandling` value per site (default is `High` only; validate whether the threshold needs to be lowered to cover the full Tier A/B population).
3. Confirm whether `ActivateExceptionClassification` will be used or not (Pfizer apparently does not use it).
4. Validate the field mapping we need from `ProcessRecordedException` (`PSUniqueExceptionID`, `OrderOrWorkflowIdentifier`, `ExceptionReference`, `ExceptionRiskChoiceElement`, `ExceptionCategoryChoiceElement`, signatures, `StationIdentifier`, `WorkCenterIdentifier`) against what is actually required for the SQL data model.
5. Confirm whether `ExceptionComment` (the Supervisor's comment on the exception) is present/populated on the outbound `ProcessRecordedException` message — the sample XMLs we've seen only carry `ExceptionDescription`, not a Supervisor comment field, so this needs to be validated as its own field mapping.

## Phase 1 — ActiveMQ discovery and setup (with Amey)

1. Get the actual broker URL and transport/protocol **for each of the 3 sites individually** (host, port, and `ssl` vs `tcp` — see the staging vs. production discrepancy noted in "Key architecture facts" above). **Still open** — see [Open questions for Pfizer Core](#open-questions-for-pfizer-core-eric-smith--solution-engineering) below.
2. Get connection credentials/certificates (TLS keystore/truststore, application service account) for the connector, per site. **Still open** — see [Open questions for Pfizer Core](#open-questions-for-pfizer-core-eric-smith--solution-engineering) below.
3. ~~Confirm the exact name of the inbound/outbound queues per site/environment.~~ **Confirmed** — standard RA product names, identical across all sites (see "Key architecture facts" above).
4. ~~Confirm the network/firewall access needed for the on-prem connector.~~ **Confirmed:** TCP port `61646` must be open (validated against the staging environment; confirm this also applies to the production endpoint once obtained).
5. ~~Confirm which integration layer sits in front of ActiveMQ at each site.~~ **Confirmed** — EiHub/EIG everywhere, same connection/message format (see "Key architecture facts" above).
6. ~~Request a staging/test ActiveMQ environment to validate the connector.~~ **Confirmed:** use Justin's internal validation environment (see "Key architecture facts" above).
7. Get the expected message volume/throughput per site. **Still open** — see [Open questions for Pfizer Core](#open-questions-for-pfizer-core-eric-smith--solution-engineering) below.

## Phase 2 — Build the on-prem connector (AMQP 1.0 → SQS)

1. Implement the connector that listens on the ActiveMQ inbound queue and republishes each message to Amazon SQS `exception-in` (S1 sub-task).
2. Design the connector to run as **one independent instance/configuration per site** (separate broker URL and credentials per site — queue names are now confirmed standard, so only the connection details vary), all republishing into the same shared SQS `exception-in` — site identity must be captured at this step (or preserved from the message) so it survives into SQL tagging (S2).
3. Handle reconnection/retries if ActiveMQ or the exception module is unavailable, without blocking PharmaSuite's standard review workflow (S1 acceptance criterion). This must be handled independently per site so an outage at one site does not affect ingestion from the others; the connector must not assume the broker will redeliver anything missed during downtime (no broker-side persistence — see "Key architecture facts" above).
4. Validate incoming XML against the `RAExternalExceptionHandling.xsd` schema. This validation logic can be shared/reused across the per-site connector instances rather than built per site (message format is identical across sites — see "Key architecture facts" above).
5. Validate the connector end-to-end against Justin's internal validation environment (`tcp://aznaampstalos1i:61646`) before requesting production broker access.

## Local Testing Infrastructure — Dev Container (added 2026-10-02)

While validating the Phase 2 connection approach against Justin's internal validation broker (`tcp://aznaampstalos1i:61646`), an AMQP 1.0 client connection attempt confirmed the broker only speaks native ActiveMQ **OpenWire** (not AMQP 1.0 or STOMP, as originally assumed in Phase 2's title). No actively maintained Python library exists for OpenWire, so a Java runtime is required to use the official `activemq-client` library — this is why a dev container (rather than a local pip install) was introduced for this testing work.

- **`Client_Projects/Pfizer/scripts/.devcontainer/`** — a VS Code dev container (JDK 17 + Python 3, Debian 12/bookworm base) so this testing setup is isolated and reproducible without installing Java/Maven on the host machine. Open this specific subfolder with **Dev Containers: Open Folder in Container...**.
- **`Client_Projects/Pfizer/scripts/activemq_test_client.py`** — round-trip connectivity test tool: drains up to N pending messages from the inbound queue (`PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST`) and publishes one placeholder `ProcessRecordedException` payload to the outbound queue (`EXCEPTION_RECORD_EXTERNAL_RESPONSE`). Originally written against `python-qpid-proton` (AMQP 1.0); after confirming the broker is OpenWire-only, it needs a Java bridge (`jpype1` + the official `activemq-client.jar`, resolved via `scripts/pom.xml`) rather than speaking OpenWire directly in Python.
- **`Client_Projects/Pfizer/scripts/java-client/`** — a second, pure-Java implementation of the same round-trip test (`ActiveMqTestClient.java`), using the official `org.apache.activemq:activemq-client` library directly via JMS, with no Python/JPype bridge involved. Kept as a parallel option to compare against the Python+JPype approach and decide which one carries forward into the real Phase 2 connector.

Both implementations are test/validation tools only, not the production connector.

## Phase 3 — SQS consumer and SQL storage

1. Build the SQS Queue Consumer (`ReceiveMessage` loop): deserialize XML, validate payload.
2. Implement the idempotency check (avoid duplicating records if the same `ProcessRecordedException` is redelivered).
3. Tag each exception with its originating site and `ORDER_NUMBER` (S2).
4. Run Archetype Resolution (`exception_type` + `risk_class`); route to the "no-archetype-match" fallback (SNS notification) when there is no match, without failing silently.
5. Design and create the target SQL schema (exceptions table, fields mapped from the XML, site, archetype, status). Store the full `ExceptionDescription` free text as-is: PharmaSuite does not expose a facts table or API, so this raw text is the only source for the underlying process facts (measured value, limit violated, etc.).
6. Write the validated records to SQL.
7. Configure a DLQ for `exception-in` and alerting on processing failures. This is our full responsibility end-to-end — no broker-side safety net to fall back on (see "Key architecture facts" above).
8. Flag as a follow-on item (outside this ingestion phase, but a direct consequence of the description-only limitation): a future AI-based step to parse `ExceptionDescription` into discrete, structured facts, since PharmaSuite will not provide these as separate fields.

## Phase 4 — End-to-end testing

1. Test with synthetic/sample exceptions in the ActiveMQ staging environment (Phase 1.5).
2. Validate the duplicate-message case (idempotency).
3. Validate the "no archetype match" case and the SNS notification.
4. Validate that the data in SQL matches the fields from the original XML.

## Phase 5 — Push-back connector (SQL/SQS → ActiveMQ → PRC visibility) — NEW, added 2026-09-29

Rahul's Sept 29 ask introduces a **second, reverse-direction** requirement that Phases 1–4 do not cover: writing processed results back into a queue so they become visible to operators in PRC (PharmaSuite's review console). This is currently underspecified — Rahul himself flagged "which queue?" as unresolved.

1. Confirm with Amey/Jonathan whether the target is the already-documented outbound queue (`EXCEPTION_RECORD_EXTERNAL_RESPONSE`) or a different one dedicated to this purpose.
2. Confirm the expected message schema/payload PRC expects on that queue (likely governed by `RAExternalExceptionHandling.xsd` as well, but needs validation — this is the response side of the same schema used for Phase 2.4, or a different one).
3. Confirm what triggers a push-back message (e.g., archetype resolved, disposition applied, AI-derived facts attached) and whether this is per-exception or per-comment.
4. Design the writer component (likely paired with the existing per-site connector from Phase 2, since it needs the same broker connection/credentials per site).
5. Validate end-to-end against Justin's/Amey's dev environment before requesting production broker access, same as the inbound flow (Phase 2.5).

## Open questions for Amey / Justin (ActiveMQ & PharmaSuite config)

Both Amey and Justin can answer in the group chat, so these are consolidated into one list (ready to post as-is). Most items either of them could answer directly are resolved (see "Key architecture facts" above): shared-vs-per-site broker, queue names, integration layer, staging environment, network/firewall access, and DLQ/retry responsibility.

**PharmaSuite configuration (Justin) — from Phase 0, not yet asked as explicit questions:**
- [ ] Can you confirm/request activation of `ActivateExternalExceptionHandling` at all 3 production sites (Andover, Grange Castle DP, Sanford)? Our understanding is it's currently disabled everywhere except the Rockwell dev instance.
- [ ] What `MinimalRiskClassForExternalExceptionHandling` value is configured (or should be configured) per site? Default is `High` only — do we need it lowered to cover the full Tier A/B population?
- [ ] Will `ActivateExceptionClassification` be used at Pfizer, or not? Our understanding is Pfizer does not use it today.

**ActiveMQ / message content (Amey) — mark which ones you can now self-test via the web console vs. which still need Amey:**
- [x] Does the `ProcessRecordedException` message carry an `ExceptionComment` field (the Supervisor's comment on the exception), separate from `ExceptionDescription`? — **Partially answered:** Amey confirmed this depends on Risk Class, but the exact rule (which Risk Class values produce a separate comment vs. embed it in `ExceptionDescription`) is still not documented. See new question below.
- [ ] **Self-testable now:** What is the exact Risk Class rule that determines whether the operator/supervisor comment arrives embedded in `ExceptionDescription` vs. as its own field/message? — try generating exceptions at different Risk Class levels in the Rockwell PS instance and browsing the resulting messages yourself; only escalate to Amey if the behavior is inconsistent or unclear.
- [x] Is `aznaampstalos1d.ra-int.com` (the "Rockwell instance", web console on `:8162`) the same broker as Justin's validation environment (`aznaampstalos1i`, `tcp://...:61646`), or a genuinely separate environment? — **Partially answered (Sept 30):** Amey described it as "our AMPS project internal development environment for one of the AMPS version having PS 10.2 version" — strongly implies a separate, purpose-built environment, but the exact relationship to Justin's environment was never asked/confirmed explicitly. Low priority to chase further unless a conflict shows up in testing.
- [ ] **Still needs Amey — blocking for Phase 2 code:** What is the message-protocol port (OpenWire/AMQP/STOMP — not the `:8162` web console) for the Rockwell instance, so we can connect a consumer programmatically? Browser access to the queues doesn't substitute for this.
- [x] Is `aznaampstalos1d.ra-int.com` reachable via Rockwell VPN, Pfizer VPN, or both? — **Confirmed (Sept 30, Amey):** Rockwell network only.
- [ ] **Lower priority, not blocking:** Rahul's "admin access" request (Sept 29) was answered with two personalized PS users (`RJ/RJ`, `NS/NS`), not a scope change. Do these accounts have admin-level PS roles, or are they standard/operator roles? And separately — can the ActiveMQ side (still shared `admin/admin`) also get personalized/lower-privilege credentials, or is `admin/admin` the only option on this dev instance?
- [ ] **Still needs Amey/Jonathan — still unresolved, not self-testable:** For the requested push-back capability, which queue should receive the response message — `EXCEPTION_RECORD_EXTERNAL_RESPONSE` (already documented) or a different one — and what payload/schema does PRC expect on it? (You can browse `EXCEPTION_RECORD_EXTERNAL_RESPONSE` now to see if any existing messages hint at the expected shape, but confirming it's the *right* queue for this new purpose still needs a person to answer.)
- [ ] **Self-testable now:** When an exception has multiple operator comments over time (not just one), does each generate its own separate ActiveMQ message instance? — generate a test exception, add a second comment, and watch whether a new message appears on `PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST`.

The remaining items (broker URL/port/protocol per site, credentials/certificates, message volume/throughput) are outside what Amey/Justin own — they've been redirected to Pfizer Core. They are tracked once, below, instead of repeating them here.

## Open questions for Jonathan Evans (MESPublishingService / API)

**Status (Sept 30):** Confirmed with Jonathan: MESPublishingService **is hosted on the same box** as the Rockwell ActiveMQ/PS instance (`aznaampstalos1d.ra-int.com`) — "Yes, the MES Pub service is hosted on that box." This is good news: no separate environment to chase, and it means once you're on the Rockwell network you may be able to explore both integration paths side by side. Next step is getting the concrete connection details below.

Jonathan is the point of contact Mike Baranski named (Sept 25) for the MESPublishingService/API path — a **different, potentially parallel** integration to the ActiveMQ `ProcessRecordedException` flow this plan is built around. Send these next, in order:

- [x] Is your "AMPS 9 (PS10.2) integration environment" the same host as `aznaampstalos1d.ra-int.com`? — **Answered (Sept 30):** Yes, confirmed same box.
- [x] Is MESPublishingService switched on in any dev or test environment today? Which one, and what version? — **Answered (Sept 30):** Yes, on `aznaampstalos1d.ra-int.com` (the Rockwell box), AMPS 9 / PS10.2.
- [ ] What are the broker URL and the queue/topic names (`PublishingEventQueue`, `PublishingPublishTopic`) on this box?
- [ ] Who can give us a test login for that broker (or does our existing `NS/NS` / `RJ/RJ` PS login also cover this service)?
- [ ] Can you send one real sample response for an order that has exceptions and comments?
- [ ] Can you send a `BatchRecord` request for one dev order and share what comes back?
- [ ] **New:** Does the response include Procedure Name, UP Name, OP Name, and Phase Identifier? These columns are on Rahul's requested field list but don't appear in the `ProcessRecordedException` samples we already have — we need to know if this API is the actual source for them, or if they come from elsewhere.

## Open questions for Pfizer Core (Eric Smith) / Solution Engineering

- [ ] Broker URL/host, port, and transport/protocol (`ssl`/61647 per the technical manual vs. `tcp`/61646 as seen in the validation environment) for each of the 3 sites (Andover, Grange Castle DP, Sanford).
- [ ] Connection credentials/certificates (TLS keystore/truststore, service account) for each site's broker.
- [ ] Expected message volume/throughput per site.
