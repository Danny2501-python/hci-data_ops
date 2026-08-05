# Pancake ingestion

## Airflow configuration

- Connection `dwh_product_new`: target PostgreSQL database.
- Variable `pancake_user_access_token`: Pancake user access token. Never commit it.
- Optional Variable `pancake_page_access_tokens`: JSON object mapping each
  `page_id` to its non-expiring `page_access_token`. When configured, customer
  ingestion enriches POS customers with the documented Page Customer API.

Example shape (placeholder values only):

```json
{"page-id-1": "page-access-token-1", "page-id-2": "page-access-token-2"}
```

The application deliberately does not regenerate page tokens during scheduled
runs because Pancake invalidates the previous token when a new one is generated.

## Contact fields

- Shop phone numbers come from `platform_extra_info.phone_number` on managed
  Pancake pages linked to the POS shop.
- Customer `phone_numbers` and `emails` retain all distinct values known by POS
  and, when page tokens are configured, Page Customer API.
- Pancake does not document a Facebook friends list or friend-count field; page
  customers are the supported contact/customer dataset.

## Webhook deployment

`pancake_webhook_listener.py` is a long-running FastAPI service. Airflow only
deploys its source to NFS; a Kubernetes Deployment/Service/Ingress must run and
expose it separately before Pancake can deliver messaging webhooks.
