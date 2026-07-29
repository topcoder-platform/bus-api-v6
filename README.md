# Topcoder Bus API v6

## Overview

This service is the NestJS and TypeScript replacement for the event, topic, and health portions of the legacy `tc-bus-api`. Bus URLs use the `/v6/bus` prefix.

## Platform

The following sibling services publish through the Bus API. The timing shown here is based on active production call sites; tests, mocks, generated files, and worktrees are excluded.

| Service | When it publishes events |
| --- | --- |
| `autopilot-v6` | After the scheduler successfully opens or closes a challenge phase, to email reviewers who opted in to phase-change notifications. |
| `challenge-api-v6` | After challenge create, update, or delete operations and after create, update, or delete operations for phases, attachments, challenge types and tracks, timeline templates, challenge timeline mappings, and default reviewers. It also publishes configured notification and email events. |
| `engagements-api-v6` | When members are assigned to private engagements, applications move to under-review or rejected, assignment offers are created or changed, and members accept or reject offers. Assignment and application notifications are sent as email events. |
| `forums-api-v6` | After a topic or post is created in a watched forum, when there are eligible watchers to notify by email. |
| `groups-api-v6` | After group create, bulk create, update, or delete operations; subgroup create or delete operations; membership additions or removals; and group-member role additions or removals. |
| `identity-api-v6` | During user registration, activation, deactivation, handle changes, and primary-email changes, and when sending password-reset, activation, activation-resend, welcome, and two-factor authentication emails. |
| `learning-paths-api` | When users enroll in, start, or complete certification and FCC learning flows; when TCA completion is recorded; and when the administrative utility explicitly submits an event. |
| `leave-api-v6` | On the last UTC day of each month, to send the monthly leave reminder email to Topcoder Staff members. |
| `marathon-match-api-v6` | After provisional/example submission scoring or final system scoring completes, to send the scoring-completion email once per submission and scoring stage. |
| `member-api-v6` | After member profile updates, including email changes, and after member-trait create, update, or delete operations. |
| `projects-api-v6` | After project create, update, billing-account update, or delete operations; member additions or removals; and invite create, update, accept, or remove operations. It also publishes project-invite and copilot workflow emails. Retained metadata publisher call sites are currently no-ops. |
| `resource-api-v6` | After challenge-resource create or delete operations and resource-role create or update operations, when resource creation requires an email notification, and when handled resource operations report an error event. |
| `review-api-v6` | When submissions are created or sent for antivirus scanning; when scanned First2Finish, Topgear, or Marathon Match submissions are released for processing; when reviews, appeal responses, or AI workflows complete; when approved review applications create resources; and when review, contact, escalation, or workflow emails are required. |
| `submission-scanner-processor` | After an antivirus scan completes or fails closed, when the request selected a Kafka callback; the result is published to the request's whitelisted callback topic. |
| `tc-finance-api` | When an OTP email is requested and when a winnings transaction needs to remind a member to finish payment or tax setup. |
| `tc-project-service` | Across the legacy project lifecycle: project, member, invite, attachment, phase, milestone, timeline, phase-product, and customer-payment changes, plus Connect notifications and copilot/member-invite emails. |
| `terms-receiver` | While processing DocuSign callbacks, including support-email requests, tax-form updates, envelope status updates, and user agreement events. |
| `terms-service` | After terms-of-use and resource-terms create, update, or delete operations, when a user agrees to terms, and when a DocuSign envelope is created. |
| `timeline-wall-api` | When an administrator approves or rejects a submitted timeline event, to email its creator. |

Other platform callers in this repository also need to move to the v6 path:

- `auth0-config` publishes `member.action.login` from the login-notifier Auth0 action after an interactive login to the Topcoder application.
- `platform-ui` publishes events when an administrator starts a Marathon Match test, requests an antivirus rescan, or reprocesses a First2Finish or Topgear submission.

`tc-mm-processor` contains an exported helper that can publish `common.error.reporting`, but there is no active in-repository call site for that helper.

Current callers use a mix of `tc-bus-api-wrapper` with a `BUSAPI_URL` base, complete endpoint variables such as `BUS_API_URL` or `BUSAPI_EVENTS_URL`, and code that constructs or hard-codes `/v5/bus/events`. During migration, verify the resolved write URL for each caller is exactly `POST /v6/bus/events`; whether the configured value is a base URL or a complete endpoint differs by service.

## Supported behavior

The API publishes complete legacy-compatible event bodies, lists Kafka topics, and reports Kafka-backed service health. Protected endpoints accept Topcoder M2M Bearer tokens validated with `AUTH_SECRET` and `VALID_ISSUERS`.

## Local setup

```bash
nvm use
pnpm install
cp .env.sample .env
pnpm start:dev
```

The service loads `.env` during startup before application configuration is
evaluated. Variables already exported by the shell take precedence over values
in the file.

## API documentation

After starting the service on the default port, the Swagger UI is available at [http://localhost:3000/v6/bus/api-docs](http://localhost:3000/v6/bus/api-docs).

## Endpoints

- `POST /v6/bus/events` requires the `write:bus_api` M2M scope and returns `202 Accepted` without a body after Kafka accepts the event.
- `GET /v6/bus/topics` requires the `read:bus_topics` M2M scope and returns the Kafka topic names. `HEAD /v6/bus/topics` requires the same scope and runs the same metadata availability path without a body.
- `GET /v6/bus/health` and `HEAD /v6/bus/health` are unauthenticated. Health reads cached Kafka lifecycle state without waiting on Kafka. A new task returns `503 Service Unavailable` until Kafka succeeds once, protecting rolling deployments from replacing a working task with an invalid Kafka configuration. After that first success, health remains available during bounded runtime reconnect attempts and returns 503 only after recovery is exhausted or shutdown starts. GET returns exactly `{ "health": "ok" }` and HEAD returns no body. Publishing remains stricter and accepts events only in the `Ready` state.

Event bodies must be JSON objects with own properties named `topic`, `originator`, `timestamp`, `mime-type`, and `payload`. Topics must match `^([a-zA-Z0-9]+\.)+[a-zA-Z0-9]+$`; the string fields follow the legacy contract, and optional `key` must be a string. Additional fields are preserved and the submitted object is published without sanitizing, renaming, or stripping properties.

## Environment variables

| Variable | Default/sample | Purpose |
| --- | --- | --- |
| `PORT` | `3000` | HTTP listen port. |
| `NODE_ENV` | `development` | Runtime environment name. |
| `AUTH_SECRET` | empty | Shared secret used by Topcoder JWT validation. |
| `VALID_ISSUERS` | Topcoder development and production issuer JSON array | Accepted JWT issuers. |
| `KAFKA_URL` | `localhost:9092` | Comma-separated Kafka broker list. |
| `KAFKA_CLIENT_ID` | `bus-api-v6` | Kafka client identifier. |
| `KAFKA_TLS_ENABLED` | `false` | Enables Kafka TLS. |
| `KAFKA_TLS_REJECT_UNAUTHORIZED` | `true` | Enables Kafka TLS certificate verification. |
| `KAFKA_SASL_MECHANISM` | empty | Kafka SASL mechanism. |
| `KAFKA_SASL_USERNAME` | empty | Kafka SASL username. |
| `KAFKA_SASL_PASSWORD` | empty | Kafka SASL password. |
| `KAFKA_CONNECTION_TIMEOUT` | `10000` | Kafka connection timeout in milliseconds. |
| `KAFKA_BROKER_TIMEOUT` | `5000` | Kafka broker operation timeout in milliseconds. |
| `KAFKA_REQUEST_TIMEOUT` | `30000` | End-to-end Kafka request timeout in milliseconds. |
| `KAFKA_METADATA_REFRESH_INTERVAL` | `60000` | Active metadata-check interval in milliseconds; must remain below the current 600000 ms broker idle timeout. |
| `KAFKA_RETRY_ATTEMPTS` | `5` | Fresh producer/admin recovery attempt budget. |
| `KAFKA_INITIAL_RETRY_TIME` | `100` | Initial Kafka recovery delay in milliseconds. |
| `KAFKA_MAX_RETRY_TIME` | `30000` | Maximum Kafka recovery delay in milliseconds. |
| `LOG_LEVEL` | `info` | Application logging level. |
| `CORS_ALLOWED_ORIGIN` | empty | Optional additional exact CORS origin. |

## Internal Kafka layer

The shared producer-focused integration uses `@platformatic/kafka` `2.8.0` with the v6-only `KAFKA_*` environment variables listed above. It owns one producer and one admin metadata client and closes both clients through Nest shutdown hooks. A background, single-flight metadata check actively verifies both clients at the configured refresh interval; failures trigger bounded reconnection and client recreation.

Health handling reads the resulting lifecycle state from memory and never performs Kafka I/O on the request path. Initial health is withheld until Kafka verifies successfully. Subsequent reconnecting is treated as healthy while recovery remains bounded, but publishing requires the producer to be fully `Ready`. Terminal recovery failure and shutdown states are unhealthy.

Publishing serializes the complete submitted event body as JSON rather than publishing only its `payload`. The optional event key is included only when supplied. Each message contains exactly the buffer-backed `originator`, `mime-type`, `timestamp`, and `topic` Kafka headers, preserving the `mime-type` spelling. Platformatic operation retries and stale-metadata replay are disabled: one HTTP publish invokes one producer send, and a transport failure is returned to the caller while client recovery prepares subsequent requests.

Topic listing always attempts a fresh all-topic metadata query. Successful results replace an in-memory last-known topic snapshot and refresh timestamp; when a later query fails, the snapshot is returned defensively. Metadata failures before any successful snapshot produce a server error from the topics endpoints.

## Validation

```bash
nvm use
pnpm lint
pnpm build
pnpm test
```
