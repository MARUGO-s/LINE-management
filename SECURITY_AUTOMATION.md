# Security Automation Plan

## Current state
- Secrets are handled outside Git via environment variables and Supabase secrets.
- No fully automated key rotation pipeline is configured yet.

## What is now automated
- Secret scanning on every push/PR and weekly schedule:
  - `.github/workflows/secret-scan.yml`
- Monthly rotation checklist issue auto-created:
  - `.github/workflows/rotation-reminder.yml`

## Rotation policy (recommended)
- Monthly:
  - `ADMIN_DASHBOARD_TOKEN`
  - `GROQ_API_KEY`
- Quarterly:
  - `LINE_CHANNEL_ACCESS_TOKEN`
  - `GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY`
- On incident / leakage suspicion:
  - Immediate rotation for all affected keys

## Notes
- Some providers require console operations or privileged API calls; fully unattended rotation may not be possible for every key.
- Keep an approval step before invalidating old production keys.
