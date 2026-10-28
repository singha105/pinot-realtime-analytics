-- On-call view: the ten tenants with the most 5xx failures on invoice.paid webhooks.
SELECT tenant_id, COUNT(*) AS failures, AVG(latency_ms) AS avg_ms
FROM {table}
WHERE event_type = 'invoice.paid' AND status_class = '5xx'
GROUP BY tenant_id
ORDER BY COUNT(*) DESC, tenant_id
LIMIT 10
