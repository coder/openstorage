> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Webhooks

> Receive real-time HTTP POST notifications when code changes and sync lifecycle events. Subscribe to push and repo sync events, verify HMAC signatures for security, and integrate Code Storage into your CI/CD pipelines, monitoring systems, and product workflows.

Code Storage provides a webhook system that allows you to receive real-time notifications about Git
events across your storage layer. This enables you to integrate Code Storage deeply within your
product, CI/CD pipelines, monitoring systems, and more.

## How webhooks work

Webhooks are HTTP POST requests sent to your specified endpoint whenever certain events occur in
your repositories. When you create a webhook subscription, Code Storage will:

1. **Monitor Events**: Watch for the events you've subscribed to (e.g., `push` or `repo.sync.*`
   events)
2. **Generate Payloads**: Create JSON payloads containing event details
3. **Sign Requests**: Add cryptographic signatures for security verification
4. **Deliver Webhooks**: Send HTTP POST requests to your endpoint with automatic retries

### Webhook headers

Every webhook request includes the following headers:

* `Content-Type: application/json`
* `User-Agent: Pierre-Webhook/1.0`
* `X-Pierre-Event: <event_type>` (e.g., `push`, `repo.sync.started`, `repo.sync.succeeded`,
  `repo.sync.failed`)
* `X-Pierre-Signature: t=1642678200,sha256=abc123...` (security signature)

## Event types

### `push`

Triggered when commits are pushed to a repository.

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "repository": {
    "id": "repo_01HZXE4K6YZ1Q2ABCD3EFG45H6",
    "url": "team/project-alpha"
  },
  "ref": "refs/heads/main",
  "before": "abc123def456...",
  "after": "def456abc123...",
  "customer_id": "your-customer-id",
  "pushed_at": "2024-01-20T10:30:00Z"
}
```

### `repo.sync.started`

Triggered when a repository sync run begins, before the upstream fetch executes. Useful for tracking
sync progress or updating status indicators in your UI.

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "type": "repo.sync.started",
  "repository": {
    "id": "repo_01HZXE4K6YZ1Q2ABCD3EFG45H6",
    "url": "team/project-alpha"
  },
  "run_count": 1,
  "is_first_sync": true,
  "started_at": "2026-03-17T10:00:00Z"
}
```

### `repo.sync.succeeded`

Triggered when a sync run completes successfully.

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "type": "repo.sync.succeeded",
  "repository": {
    "id": "repo_01HZXE4K6YZ1Q2ABCD3EFG45H6",
    "url": "team/project-alpha"
  },
  "run_count": 1,
  "is_first_sync": true,
  "started_at": "2026-03-17T10:00:00Z",
  "completed_at": "2026-03-17T10:02:30Z"
}
```

### `repo.sync.failed`

Triggered when a sync run fails, including mirror activity failures (after retries are exhausted)
and infrastructure-level failures.

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "type": "repo.sync.failed",
  "repository": {
    "id": "repo_01HZXE4K6YZ1Q2ABCD3EFG45H6",
    "url": "team/project-alpha"
  },
  "run_count": 1,
  "is_first_sync": true,
  "started_at": "2026-03-17T10:00:00Z",
  "completed_at": "2026-03-17T10:02:30Z",
  "error": "authentication failed"
}
```

### Sync event fields

| Field           | Description                                                                                                                                                                                                                                                                        |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `run_count`     | For `started`, the number of previously completed runs. For `succeeded`/`failed`, includes the run that just finished.                                                                                                                                                             |
| `is_first_sync` | `true` if the repository had never successfully synced before this run began. Useful for detecting initial sync completion without comparing run counts.                                                                                                                           |
| `error`         | Present only on `failed` events. Possible values: `"authentication failed"`, `"failed to clone repository"`, `"failed to push to storage"`, `"repository configuration error"`, `"upstream repository unreachable"`, `"internal error"`, `"session unavailable"`, `"sync failed"`. |

<Note>
  Cancellations triggered internally (e.g., when a repository is detached from its upstream) do not
  produce a `repo.sync.failed` event.
</Note>

## Securing webhooks

To ensure the webhooks you receive are legitimate and from Code Storage, you **must** verify the
HMAC signature included with each webhook delivery.

### HMAC Signature Verification

Each webhook includes an `X-Pierre-Signature` header with the format:

```
X-Pierre-Signature: t=<unix_timestamp>,sha256=<hex_signature>
```

The signature is computed as:

```
HMAC-SHA256(webhook_secret, timestamp + "." + payload)
```

## Webhook SDK methods

The SDK provides helper methods to help you validate webhook events quickly.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { validateWebhook } from '@pierre/storage';

  async function handleWebhookRequest(request) {
    // Get the raw request body as text
    const payload = await request.text();

    // Validate the webhook using the SDK
    const result = await validateWebhook(
      payload,
      request.headers, // Headers object with x-pierre-signature and x-pierre-event
      process.env.WEBHOOK_SECRET,
    );

    if (!result.valid) {
      console.error('Invalid webhook:', result.error);
      return new Response('Invalid webhook', { status: 401 });
    }

    // Route by event type
    switch (result.eventType) {
      case 'push':
        console.log(`Push to ${result.payload.ref}`);
        console.log(`Commit: ${result.payload.before} -> ${result.payload.after}`);
        break;
      case 'repo.sync.started':
      case 'repo.sync.succeeded':
      case 'repo.sync.failed': {
        // Sync events are delivered as raw JSON — parse the validated payload
        const sync = JSON.parse(payload);
        console.log(`${result.eventType} for ${sync.repository.id}`);
        if (sync.error) {
          console.log(`Error: ${sync.error}`);
        }
        break;
      }
    }

    return new Response('OK', { status: 200 });
  }
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import json
  from pierre_storage import validate_webhook

  # Validate webhook signature
  result = validate_webhook(
      payload=request.body,  # Raw payload bytes or string
      headers={
          "X-Pierre-Signature": request.headers["X-Pierre-Signature"],
          "X-Pierre-Event": request.headers["X-Pierre-Event"],
      },
      secret="your-webhook-secret",
      options={"max_age_seconds": 300},  # 5 minutes
  )

  if not result["valid"]:
      print(f"Invalid webhook: {result.get('error')}")
  else:
      event_type = result.get("event_type")

      if event_type == "push":
          event = result.get("payload")
          print(f"Push to {event['ref']}")
          print(f"Commit: {event['before']} -> {event['after']}")
      elif event_type in ("repo.sync.started", "repo.sync.succeeded", "repo.sync.failed"):
          # Sync events are delivered as raw JSON — parse the validated payload
          sync = json.loads(request.body)
          print(f"{event_type} for {sync['repository']['id']}")
          if sync.get("error"):
              print(f"Error: {sync['error']}")
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Read the raw request body
  payload := requestBody // []byte

  // Collect webhook headers
  headers := http.Header{
  	"X-Pierre-Signature": []string{r.Header.Get("X-Pierre-Signature")},
  	"X-Pierre-Event":     []string{r.Header.Get("X-Pierre-Event")},
  }

  // Validate the webhook signature
  result := storage.ValidateWebhook(
  	payload,
  	headers,
  	"your-webhook-secret",
  	storage.WebhookValidationOptions{MaxAgeSeconds: 300},
  )

  if !result.Valid {
  	fmt.Printf("Invalid webhook: %s", result.Error)
  	return
  }

  // Route by event type
  switch result.EventType {
  case "push":
  	event := result.Payload.Push
  	fmt.Printf("Push to %s", event.Ref)
  	fmt.Printf("Commit: %s -> %s", event.Before, event.After)
  case "repo.sync.started", "repo.sync.succeeded", "repo.sync.failed":
  	// Sync events are delivered as raw JSON — unmarshal the validated payload
  	var sync struct {
  		Repository struct {
  			ID  string `json:"id"`
  			URL string `json:"url"`
  		} `json:"repository"`
  		RunCount    int    `json:"run_count"`
  		IsFirstSync bool   `json:"is_first_sync"`
  		StartedAt   string `json:"started_at"`
  		CompletedAt string `json:"completed_at,omitempty"`
  		Error       string `json:"error,omitempty"`
  	}
  	json.Unmarshal(payload, &sync)
  	fmt.Printf("%s for %s", result.EventType, sync.Repository.ID)
  	if sync.Error != "" {
  		fmt.Printf("Error: %s", sync.Error)
  	}
  }
  ```
</CodeGroup>

### Advanced SDK usage

Custom Validation Options:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { validateWebhook } from '@pierre/storage';

  const result = await validateWebhook(payload, headers, webhookSecret, {
    maxAgeSeconds: 600, // Allow webhooks up to 10 minutes old (default: 300)
    // maxAgeSeconds: 0  // Disable timestamp validation entirely
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  from pierre_storage import validate_webhook

  result = validate_webhook(
      payload=payload,
      headers=headers,
      secret=webhook_secret,
      options={"max_age_seconds": 600},  # 10 minutes (default: 300)
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  result := storage.ValidateWebhook(
  	payload,
  	headers,
  	webhookSecret,
  	storage.WebhookValidationOptions{MaxAgeSeconds: 600},
  )
  ```
</CodeGroup>

Signature-Only Validation for cases where you need more control over the validation process:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { validateWebhookSignature, parseSignatureHeader } from '@pierre/storage';

  // Just validate the signature without parsing the payload
  const result = await validateWebhookSignature(
    payload,
    headers['x-pierre-signature'],
    webhookSecret,
  );

  if (result.valid) {
    // Manually parse and process the payload as needed
    const eventType = headers['x-pierre-event'];
    const webhookData = JSON.parse(payload);
    // ... custom processing logic
  }
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  from pierre_storage import validate_webhook_signature, parse_signature_header

  # Just validate the signature without parsing the payload
  result = validate_webhook_signature(
      payload=payload,
      signature_header=headers.get("X-Pierre-Signature"),
      secret=webhook_secret,
  )

  if result["valid"]:
      # Manually parse and process the payload as needed
      parsed = parse_signature_header(headers.get("X-Pierre-Signature"))
      event_type = headers.get("X-Pierre-Event")
      webhook_data = json.loads(payload)
      # ... custom processing logic
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Just validate the signature without parsing the payload
  result := storage.ValidateWebhookSignature(
  	payload,
  	headers.Get("X-Pierre-Signature"),
  	webhookSecret,
  	storage.WebhookValidationOptions{},
  )

  if result.Valid {
  	// Manually parse and process the payload as needed
  	parsed := storage.ParseSignatureHeader(headers.Get("X-Pierre-Signature"))
  	eventType := headers.Get("X-Pierre-Event")
  	_ = parsed
  	_ = eventType
  	// ... custom processing logic
  }
  ```
</CodeGroup>

### Common verification errors

When using the SDK, these errors are automatically detected and returned in the `result.error`
field:

* **Missing signature components**: "Invalid signature header format"
* **Timestamp too old**: "Webhook timestamp too old (X seconds)"
* **Future timestamp**: "Webhook timestamp is in the future"
* **Signature mismatch**: "Invalid signature"
* **Invalid JSON**: "Invalid JSON payload" (when using `validateWebhook`)
* **Missing headers**: "Missing or invalid X-Pierre-Signature header"

**Error Handling Best Practices:**

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { validateWebhook } from '@pierre/storage';

  const result = await validateWebhook(payload, headers, secret);

  if (!result.valid) {
    // Log the error for debugging (don't expose to clients)
    console.error('Webhook validation failed:', result.error, {
      timestamp: result.timestamp,
      eventType: result.eventType,
      // Don't log payload or secret for security
    });

    // Return appropriate HTTP status codes
    if (result.error?.includes('timestamp')) {
      return new Response('Request too old', { status: 408 }); // Request Timeout
    }

    return new Response('Invalid webhook signature', { status: 401 });
  }
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  from pierre_storage import validate_webhook

  result = validate_webhook(payload, headers, secret)

  if not result["valid"]:
      # Log the error for debugging (don't expose to clients)
      print("Webhook validation failed:", result.get("error"))

      # Return appropriate HTTP status codes
      if "timestamp" in (result.get("error") or ""):
          return Response("Request too old", status=408)

      return Response("Invalid webhook signature", status=401)
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  result := storage.ValidateWebhook(payload, headers, secret, storage.WebhookValidationOptions{})

  if !result.Valid {
  	// Log the error for debugging (don't expose to clients)
  	fmt.Printf("Webhook validation failed: %s\n", result.Error)

  	// Return appropriate HTTP status codes
  	if strings.Contains(result.Error, "timestamp") {
  		// Request Timeout
  		_ = 408
  	} else {
  		// Unauthorized
  		_ = 401
  	}
  }
  ```
</CodeGroup>
