Those are the API operations exposed by the app. Swagger lists every HTTP route you can call to manage a Zoho connection and move data.

Providers: inspect available integrations and their supported streams, e.g. Zoho contacts, deals.
Connections: create and manage a saved Zoho CRM connection. Start with POST /v1/connections.
OAuth: authorize that connection with Zoho. After creating a connection, use GET /v1/oauth/zoho_crm/authorize with its connection_id, then approve access in the browser.
Sync: pull records from Zoho into this app. Use POST /v1/connections/{connection_id}/sync.
Staged Records: read records that were pulled and stored locally.
Push / Outbox: send canonical records from this app to Zoho, either immediately or through the retryable queue.
Webhooks: register Zoho change notifications and receive them at the public webhook endpoint.
Ops: GET /health confirms the API is running.