"""
LOAD BALANCER FOR SHOPIFY CHECK API

"""

from __future__ import annotations
import asyncio
import aiohttp
import time
import re
import random
import threading
from dataclasses import dataclass, field
from yarl import URL as _URL

# ── Endpoint + SSH config ─────────────────────────────────────────────────────
_NODE_CONFIGS = [
    {
        "name":     "HRK-1",
        "url":      "https://shopi1-17f991bdc4bf.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-2",
        "url":      "https://shopify-chk-api-1-5447c651d0e0.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-3",
        "url":      "https://shopify-chk-api-2-a6a5eec1087f.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-4",
        "url":      "https://shopify-chk-api-3-4f2a2591c9ec.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-5",
        "url":      "https://shopify-chk-api-4-c73929c99b42.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-6",
        "url":      "https://shopify-chk-api-5-11bcbe435957.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-7",
        "url":      "https://shopify-chk-api-6-c9a97cb5c077.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-8",
        "url":      "https://young-wildwood-69633-589ff1a13e20.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-9",
        "url":      "https://tranquil-river-48797-94a24658354a.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-10",
        "url":      "https://sheltered-shelf-19998-bcba934b85fa.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-11",
        "url":      "https://limitless-oasis-99939-a535b5c95a02.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-12",
        "url":      "https://whispering-oasis-87875-03f50725eabc.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-13",
        "url":      "https://radiant-waters-24994-36c669d0b1da.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
    {
        "name":     "HRK-14",
        "url":      "https://arcane-harbor-54948-a2ec8a57f0e3.herokuapp.com",
        "ssh_host": "",
        "ssh_port": 0,
        "ssh_user": "",
        "ssh_pass": "",
        "type":     "render",
        "weight":   1.0,
    },
]

_CHECK_TIMEOUT      = 90     # per-request timeout — Render can be slow (seconds)
_VPS_CHECK_TIMEOUT  = 75     # Go API has 50s internal deadline + network overhead
_HEALTH_INTERVAL    = 3      # health-loop tick (seconds) — faster for fresher load data
_PROBE_TIMEOUT      = 15     # /health probe timeout — allow Render cold-start
_VPS_PROBE_TIMEOUT  = 8      # faster probe for VPS

# Escalating cooldown steps per node type (seconds)
_COOLDOWN_STEPS_VPS    = [15, 30, 60]    # VPS: shorter — they recover fast
_COOLDOWN_STEPS_RENDER = [30, 60, 120]   # Render: longer — cold starts are slow


# ── Per-node runtime state ────────────────────────────────────────────────────
@dataclass
class _Node:
    name:     str
    url:      str
    ssh_host: str
    ssh_port: int
    ssh_user: str
    ssh_pass: str
    node_type: str   # "vps" | "render"
    weight:    float

    enabled:              bool  = True
    active:               int   = 0      # in-flight local counter
    workers_busy:         int   = 0      # from /health
    workers_cap:          int   = 1000   # from /health (updated live); fallback estimate
    total:                int   = 0      # lifetime requests routed
    fail_streak:          int   = 0      # consecutive errors
    timeout_streak:       int   = 0      # consecutive timeouts (separate from errors)
    consecutive_timeouts: int   = 0      # drives cooldown escalation (cumulative)
    cooldown_until:       float = 0.0
    health_ts:            float = 0.0
    probing:              bool  = False
    last_ok:              float = field(default_factory=time.monotonic)
    avg_rtt_ms:           float = 0.0    # EWMA response time (ms)
    warmed:               bool  = False

    # ── derived timeouts (fixes AttributeError that silently broke health polls) ──
    @property
    def probe_timeout(self) -> int:
        return _VPS_PROBE_TIMEOUT if self.node_type == "vps" else _PROBE_TIMEOUT

    @property
    def check_timeout(self) -> int:
        return _VPS_CHECK_TIMEOUT if self.node_type == "vps" else _CHECK_TIMEOUT

    @property
    def on_cooldown(self) -> bool:
        return time.monotonic() < self.cooldown_until

    @property
    def cooldown_expired(self) -> bool:
        return (
            self.cooldown_until > 0.0
            and not self.on_cooldown
            and not self.probing
        )

    @property
    def available(self) -> bool:
        return self.enabled and not self.on_cooldown and not self.probing

    @property
    def cooldown_remaining(self) -> int:
        return max(0, int(self.cooldown_until - time.monotonic()))

    # ── mutations ─────────────────────────────────────────────────────────────
    def start_cooldown(self):
        """Escalating cooldown — VPS steps are shorter than Render."""
        self.consecutive_timeouts += 1
        steps = _COOLDOWN_STEPS_VPS if self.node_type == "vps" else _COOLDOWN_STEPS_RENDER
        idx   = min(self.consecutive_timeouts - 1, len(steps) - 1)
        secs  = steps[idx]
        self.cooldown_until = time.monotonic() + secs
        print(
            f"[NODE] {self.name} → cooldown {secs}s "
            f"(streak={self.consecutive_timeouts}, type={self.node_type})",
            flush=True,
        )

    def mark_ok(self):
        self.last_ok              = time.monotonic()
        self.fail_streak          = 0
        self.timeout_streak       = 0
        self.consecutive_timeouts = 0
        self.cooldown_until       = 0.0
        self.probing              = False
        self.warmed               = True

    def update_rtt(self, elapsed_ms: float):
        """EWMA of response time — α=0.2 (new) + 0.8 (old)."""
        if self.avg_rtt_ms == 0.0:
            self.avg_rtt_ms = elapsed_ms
        else:
            self.avg_rtt_ms = 0.8 * self.avg_rtt_ms + 0.2 * elapsed_ms

    def record_timeout(self):
        """
        Timeout policy:
          VPS:    tolerate 1 isolated timeout (might just be a slow card);
                  cooldown only on 2nd consecutive timeout.
          Render: 1 timeout → cooldown (cold starts are node-level, not card-level).
        """
        self.active = max(0, self.active - 1)
        self.fail_streak    += 1
        self.timeout_streak += 1
        threshold = 2 if self.node_type == "vps" else 1
        if self.timeout_streak >= threshold:
            self.timeout_streak = 0
            self.start_cooldown()
        else:
            print(
                f"[NODE] {self.name} timeout (streak {self.timeout_streak}/{threshold}) "
                f"— tolerating, shifting card to next node",
                flush=True,
            )

    def record_error(self):
        """Non-timeout error — cooldown after 3 for VPS, 2 for Render."""
        self.active = max(0, self.active - 1)
        self.fail_streak    += 1
        self.timeout_streak  = 0   # reset timeout streak on non-timeout error
        threshold = 3 if self.node_type == "vps" else 2
        if self.fail_streak >= threshold:
            self.start_cooldown()


# ── Proxy format helper ───────────────────────────────────────────────────────
def _to_api_proxy(proxy: str) -> str:
    p = proxy.strip()
    if not p:
        return ""
    p_clean = re.sub(r'^(https?|socks[45])://', '', p)
    if '@' in p_clean:
        creds, hostport = p_clean.rsplit('@', 1)
        parts = hostport.split(':')
        host  = parts[0]
        port  = parts[1] if len(parts) > 1 else "8080"
        cp    = creds.split(':', 1)
        user  = cp[0]
        pw    = cp[1] if len(cp) > 1 else ""
        return f"{host}:{port}:{user}:{pw}"
    return p_clean


# ── NodeManager ───────────────────────────────────────────────────────────────
class NodeManager:
    """
    Balances checks across 7 nodes (2 VPS + 5 Render).

    Selection:   VPS preferred (lower weight), then Render for overflow.
    Failure:     timeout → immediate escalating cooldown; repeated errors → cooldown.
    Recovery:    health-loop probes after cooldown; traffic only resumes on probe pass.
    Fallback:    if ALL nodes are cooling, pick enabled node with soonest recovery.
    Batch:       supports high concurrency — 7 nodes × 2000 workers = 14,000 cap.
    """

    def __init__(self, configs: list[dict]):
        self._nodes = [
            _Node(
                name=c["name"],
                url=c["url"].rstrip('/'),
                ssh_host=c.get("ssh_host", ""),
                ssh_port=c.get("ssh_port", 0),
                ssh_user=c.get("ssh_user", ""),
                ssh_pass=c.get("ssh_pass", ""),
                node_type=c.get("type", "render"),
                weight=c.get("weight", 2.0),
            )
            for c in configs
        ]
        self._session: aiohttp.ClientSession | None = None
        self._hc_task: asyncio.Task | None = None

    # ── aiohttp session ───────────────────────────────────────────────────────
    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            conn = aiohttp.TCPConnector(
                limit=0,
                limit_per_host=0,
                ssl=False,
                ttl_dns_cache=300,
                use_dns_cache=True,
            )
            self._session = aiohttp.ClientSession(
                connector=conn,
                connector_owner=True,
            )
        return self._session

    # ── Node selection ────────────────────────────────────────────────────────
    def _pick(self, exclude: set[str] | None = None) -> _Node | None:
        pool = [n for n in self._nodes if not (exclude and n.url in exclude)]
        if not pool:
            pool = list(self._nodes)

        # Primary: enabled + available
        avail = [n for n in pool if n.available]
        if not avail:
            # All cooling — pick enabled node with soonest recovery
            avail = [n for n in pool if n.enabled]
            if avail:
                avail.sort(key=lambda n: n.cooldown_until)
            else:
                avail = pool

        def _load(n: _Node) -> float:
            now = time.monotonic()
            cap = max(n.workers_cap, 1)
            # Use fresh health data if available (< 30s); else fall back to local counter
            if now - n.health_ts < 30:
                util = min(n.workers_busy / cap, 1.0)
            else:
                util = min(n.active / cap, 1.0)
            # Node-type weight: VPS (1.0) preferred over Render (2.0)
            score = util * n.weight
            # Failure streak penalty — prefer nodes with clean records
            if n.fail_streak > 0:
                score += min(n.fail_streak * 0.08, 0.5)
            # RTT penalty — prefer faster nodes (normalise to ≈0–0.3 range)
            if n.avg_rtt_ms > 0:
                score += min(n.avg_rtt_ms / 60_000, 0.3)
            # Small jitter to prevent thundering-herd on identical scores
            return score + random.uniform(0, 0.01)

        avail.sort(key=_load)
        return avail[0]

    # ── Health / recovery loop ────────────────────────────────────────────────
    async def _probe_node(self, node: _Node, sess: aiohttp.ClientSession) -> bool:
        try:
            async with sess.get(
                f"{node.url}/health",
                timeout=aiohttp.ClientTimeout(total=node.probe_timeout),
                ssl=None,   # allow self-signed on VPS
            ) as r:
                if r.status < 500:
                    try:
                        hdata = await r.json(content_type=None)
                        node.workers_busy = int(hdata.get("workers_busy", 0))
                        node.workers_cap  = int(hdata.get("workers_cap",  1000))
                        node.health_ts    = time.monotonic()
                    except Exception:
                        pass
                    return True
        except Exception:
            pass
        return False

    async def _health_loop(self):
        # Stagger initial probes so all 7 nodes don't hit simultaneously
        await asyncio.sleep(2)
        while True:
            sess = await self._sess()

            for node in self._nodes:
                if not node.enabled:
                    continue

                # Cooldown expired — probe before resuming
                if node.cooldown_expired:
                    node.probing = True
                    ok = await self._probe_node(node, sess)
                    if ok:
                        node.mark_ok()
                        print(
                            f"[NODE] {node.name} ✓ recovered — traffic resumed",
                            flush=True,
                        )
                    else:
                        node.probing = False
                        node.fail_streak += 1
                        node.start_cooldown()
                        print(
                            f"[NODE] {node.name} ✗ probe failed — "
                            f"re-cooling ({node.cooldown_remaining}s)",
                            flush=True,
                        )
                    continue

                # Normal health poll on live nodes
                if node.available:
                    try:
                        async with sess.get(
                            f"{node.url}/health",
                            timeout=aiohttp.ClientTimeout(total=node.probe_timeout),
                            ssl=None,
                        ) as r:
                            if r.status < 500:
                                try:
                                    hdata = await r.json(content_type=None)
                                    node.workers_busy = int(hdata.get("workers_busy", 0))
                                    node.workers_cap  = int(hdata.get("workers_cap",  1000))
                                    node.health_ts    = time.monotonic()
                                    node.warmed       = True
                                except Exception:
                                    pass
                                node.fail_streak = max(0, node.fail_streak - 1)
                    except Exception:
                        node.fail_streak += 1
                        if node.fail_streak >= 5:
                            node.start_cooldown()

            await asyncio.sleep(_HEALTH_INTERVAL)

    def start_health_polling(self):
        if self._hc_task is None or self._hc_task.done():
            self._hc_task = asyncio.create_task(self._health_loop())

    # ── Single check ──────────────────────────────────────────────────────────
    async def check(
        self,
        card: str,
        proxy: str = "",
        site: str  = "",
        *,
        retries: int = 6,    # try up to 7 nodes before giving up
    ) -> dict:
        """
        Route card check to least-loaded available node.

        On timeout   → node recorded (may cool down), CC immediately shifted to
                       next available node — card is NEVER dropped on timeout.
        On 503       → node overloaded, try next node softly (no cooldown counter).
        On error ×N  → escalating cooldown on that node, traffic shifts to others.
        Tries up to (retries+1) different nodes before returning failure.
        """
        proxy_str = _to_api_proxy(proxy)
        tried:    set[str] = set()   # exclude timed-out / errored nodes for this card
        last_err  = "NODE_UNAVAILABLE"
        sess      = await self._sess()

        for attempt in range(retries + 1):
            node = self._pick(exclude=tried)
            if node is None:
                break
            tried.add(node.url)

            raw_url = f"{node.url}/?{card}"
            if proxy_str:
                raw_url += f"&proxy={proxy_str}"

            node.active += 1
            node.total  += 1
            t_start = time.monotonic()

            try:
                async with sess.get(
                    _URL(raw_url, encoded=True),
                    timeout=aiohttp.ClientTimeout(total=node.check_timeout),
                    ssl=None,
                ) as r:
                    if r.status == 503:
                        # 503 = node at capacity OR no sites loaded.
                        # Either way it's NOT a node failure — don't penalise fail_streak.
                        # Just release the active slot and route to next available node.
                        node.active = max(0, node.active - 1)
                        last_err = f"503 on {node.name}"
                        print(f"[NODE] {node.name} 503 (attempt {attempt+1}) → at capacity, routing to next node", flush=True)
                        await asyncio.sleep(0.1)   # tiny pause before picking next node
                        continue

                    body = await r.json(content_type=None)
                    elapsed_ms = (time.monotonic() - t_start) * 1000
                    node.active = max(0, node.active - 1)
                    node.mark_ok()
                    node.update_rtt(elapsed_ms)

                    resp_val = str(body.get("Response", body.get("message", "UNKNOWN")))
                    gate_val = str(body.get("Gate",     body.get("gateway", "Shopify Payments")))
                    chg_val  = str(body.get("Charged",  "False")).strip().lower() == "true"
                    apv_val  = str(body.get("Approved", "False")).strip().lower() == "true"
                    p_raw    = str(body.get("Price",    body.get("price", "0")))
                    p_val    = p_raw.split()[0]
                    cur_val  = p_raw.split()[-1] if len(p_raw.split()) > 1 else "USD"

                    return {
                        "Response": resp_val, "Gate": gate_val,
                        "Charged":  str(chg_val), "Approved": str(apv_val),
                        "Price": p_raw, "Time": body.get("Time", ""),
                        "success":  chg_val or apv_val,
                        "message":  resp_val,
                        "gateway":  gate_val,
                        "price":    p_val,
                        "currency": cur_val,
                        "_node":    node.name,
                        "_rtt_ms":  round(elapsed_ms),
                    }

            except asyncio.TimeoutError:
                node.record_timeout()
                print(
                    f"[NODE] {node.name} TIMEOUT (attempt {attempt+1}/{retries+1}) "
                    f"→ shifting CC to next node (cooldown={node.cooldown_remaining}s)",
                    flush=True,
                )
                last_err = f"Timeout on {node.name}"
                # CC is NOT dropped — loop continues with next node in `tried` excluded

            except Exception as e:
                node.record_error()
                last_err = str(e)[:80]
                print(f"[NODE] {node.name} error (attempt {attempt+1}): {last_err}", flush=True)

        return {"success": False, "message": last_err}

    # ── Owner controls ────────────────────────────────────────────────────────
    def set_enabled(self, node_name: str, enabled: bool):
        for n in self._nodes:
            if n.name == node_name:
                n.enabled = enabled
                if enabled:
                    n.cooldown_until       = 0.0
                    n.probing              = False
                    n.consecutive_timeouts = 0
                print(f"[NODE] {node_name} {'enabled' if enabled else 'disabled'}", flush=True)

    async def restart_node(self, node_name: str) -> str:
        """SSH into VPS node and restart checker service. Render nodes cannot be SSH'd."""
        node = next((n for n in self._nodes if n.name == node_name), None)
        if not node:
            return "Node not found"
        if node.node_type == "render":
            return "Render nodes do not support SSH restart — they auto-recover"
        if not node.ssh_host:
            return "No SSH config for this node"
        try:
            import paramiko
            loop = asyncio.get_event_loop()
            def _ssh_restart():
                c = paramiko.SSHClient()
                c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                c.connect(node.ssh_host, port=node.ssh_port,
                          username=node.ssh_user, password=node.ssh_pass, timeout=10)
                _, pid_out, _ = c.exec_command(
                    "systemctl list-units --type=service --state=running --no-legend | awk '{print $1}'"
                    " | xargs -I{} sh -c 'systemctl cat {} 2>/dev/null | grep -q 8081 && echo {}'",
                    timeout=8,
                )
                svc_candidates = [s.strip() for s in pid_out.read().decode().splitlines() if s.strip()]
                fallbacks = ["checker-node", "checker", "shopify-checker", "api", "main"]
                svc = svc_candidates[0] if svc_candidates else None
                if not svc:
                    for fb in fallbacks:
                        _, chk, _ = c.exec_command(f"systemctl is-active {fb} 2>/dev/null", timeout=4)
                        if chk.read().decode().strip() in ("active", "activating"):
                            svc = fb
                            break
                if not svc:
                    c.close()
                    return "service-not-found"
                _, _out, _ = c.exec_command(f"systemctl restart {svc}", timeout=15)
                _out.channel.recv_exit_status()
                _, st_out, _ = c.exec_command(f"systemctl is-active {svc}", timeout=5)
                status = st_out.read().decode().strip()
                c.close()
                return f"{svc}: {status}"
            status = await loop.run_in_executor(None, _ssh_restart)
            node.mark_ok()
            return "active" if "active" in status else status
        except Exception as e:
            return f"Error: {e}"

    async def fetch_stats(self, node_name: str) -> dict:
        node = next((n for n in self._nodes if n.name == node_name), None)
        if not node:
            return {"error": "Node not found"}
        sess = await self._sess()
        try:
            async with sess.get(
                f"{node.url}/stats",
                timeout=aiohttp.ClientTimeout(total=10),
                ssl=None,
            ) as r:
                data = await r.json(content_type=None)
                if "cpu_pct" in data or "ram_total_gb" in data:
                    return data
        except Exception:
            pass
        try:
            async with sess.get(
                f"{node.url}/health",
                timeout=aiohttp.ClientTimeout(total=10),
                ssl=None,
            ) as r:
                h = await r.json(content_type=None)
                return {
                    "active_checks": h.get("workers_busy", 0),
                    "workers_cap":   h.get("workers_cap",  1000),
                    "sites":         h.get("sites", 0),
                    "_partial":      True,
                }
        except Exception as e:
            return {"error": str(e)[:80]}

    # ── Status summary ────────────────────────────────────────────────────────
    def status(self) -> list[dict]:
        return [
            {
                "name":       n.name,
                "url":        n.url,
                "type":       n.node_type,
                "enabled":    n.enabled,
                "available":  n.available,
                "on_cooldown":n.on_cooldown,
                "probing":    n.probing,
                "active":     n.active,
                "cooldown":   n.cooldown_remaining,
                "total":      n.total,
                "fails":      n.fail_streak,
                "timeouts":   n.consecutive_timeouts,
                "warmed":     n.warmed,
            }
            for n in self._nodes
        ]

    def get_nodes(self) -> list[_Node]:
        return self._nodes

    def total_capacity(self) -> int:
        return sum(n.workers_cap for n in self._nodes if n.available)

    def total_load(self) -> int:
        return sum(n.active for n in self._nodes)

    def name_of(self, node: _Node) -> str:
        return node.name


# ── Singleton ─────────────────────────────────────────────────────────────────
NODE_MANAGER = NodeManager(_NODE_CONFIGS)
