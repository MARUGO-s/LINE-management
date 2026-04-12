# Security Automation Plan

## Current state
- Secrets are managed outside Git via Supabase Secrets / Vault.
- DB cron invokers resolve token in this order:
  1. `custom.cron_auth_token`
  2. `vault` secret `CRON_AUTH_TOKEN`
  3. `vault` secret `SUPABASE_ANON_KEY`
- Hardcoded JWT/token fallback in migrations was removed.

## Automated controls
- Secret scanning:
  - `.github/workflows/secret-scan.yml` (push / PR / scheduled)
- Rotation reminder:
  - `.github/workflows/rotation-reminder.yml` (monthly issue)
- Shared rate limiting (DB-backed, cross-instance):
  - Table: `public.security_rate_limits`
  - RPC: `public.consume_security_rate_limit(...)`
  - Daily cleanup job:
    - `security-rate-limit-cleanup-job`
    - `17 3 * * *` (UTC)
    - `select public.cleanup_security_rate_limits(interval '2 days');`

## Rotation policy (recommended)
- Monthly:
  - `ADMIN_DASHBOARD_TOKEN`
  - `GROQ_API_KEY`
  - `CRON_AUTH_TOKEN`
- Quarterly:
  - `LINE_CHANNEL_ACCESS_TOKEN`
  - `GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY`
- Incident response:
  - Rotate all potentially exposed secrets immediately

## Operational checks
- Verify cron token resolution:
  - SQL: `select (public.resolve_edge_cron_auth_token() is not null) as has_cron_token;`
- Verify cleanup cron registration:
  - SQL: `select jobname, schedule, command from cron.job where jobname = 'security-rate-limit-cleanup-job';`
- Verify rate limiter activity:
  - SQL: `select bucket, window_start, hit_count, updated_at from public.security_rate_limits order by updated_at desc limit 100;`

## Notes
- Some provider keys require manual console operations; fully unattended rotation is not always possible.
- Keep approval before invalidating currently active production keys.
- Manual hygiene check command:
  - `rg "eyJhbGciOiJIUzI1Ni|-----BEGIN|sk-|ghp_" supabase/migrations supabase/functions`
