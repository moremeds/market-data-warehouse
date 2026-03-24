# IB Gateway — Docker

Runs IB Gateway via [gnzsnz/ib-gateway-docker](https://github.com/gnzsnz/ib-gateway-docker) with file-based password secrets and persistent settings.

## Setup

```bash
cd docker/ib-gateway
cp .env.example .env
# Edit .env — set TWS_USERID to your IB username
mkdir -p secrets
echo "YOUR_IB_PASSWORD" > secrets/ib_password.txt
```

## Start

```bash
docker compose up -d
```

## Complete 2FA

On first login (and after extended disconnections), IB requires two-factor authentication.

**Option A — VNC** (requires `VNC_SERVER_PASSWORD` set in `.env`):
1. Connect a VNC client to `localhost:5900`
2. Complete the IBKR 2FA prompt in the Gateway GUI

**Option B — IBKR mobile app**: Approve the 2FA push notification from your IBKR mobile app.

Subsequent restarts reuse the saved session and skip 2FA.

## Verify

```bash
docker compose ps          # STATUS should show "healthy" after ~2 minutes
docker compose logs -f     # Watch login progress
```

## Switch to live trading

Edit `.env`:
```
TRADING_MODE=live
```

Then restart:
```bash
docker compose up -d
```

## Port mapping

SOCAT inside the container relays from 0.0.0.0 to IB Gateway's localhost-only API:

| Host port | Container port | SOCAT relays to | Purpose |
|-----------|---------------|-----------------|---------|
| 4001 | 4003 | 127.0.0.1:4001 | Live trading API |
| 4002 | 4004 | 127.0.0.1:4002 | Paper trading API |
| 5900 | 5900 | — | VNC (opt-in) |

## Monitoring

- **VNC**: `localhost:5900` (set `VNC_SERVER_PASSWORD` in `.env` to enable)
- **Logs**: `docker compose logs -f`

## Stop

```bash
docker compose down
```

Gateway settings (Jts directory) are persisted in a Docker volume and survive container restarts.
