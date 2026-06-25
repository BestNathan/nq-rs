#!/usr/bin/env python3
# /// script
# dependencies = ["paho-mqtt>=2.0"]
# ///
"""
Deribit Option Monitor — MQTT WebSocket 统计客户端

连接 EMQX (via Higress Ingress MQTT-WS)，监听 1 分钟，
统计收到的期权 ticker 消息数量和涉及的期权合约数。

Usage:
    uv run ws_monitor.py
    uv run ws_monitor.py --host 192.168.2.86 --port 31693 --duration 60
    uv run ws_monitor.py --topic "t/deribit/option_ticker/BTC-*"
"""

import argparse
import json
import signal
import sys
import time
from collections import defaultdict

import paho.mqtt.client as mqtt


class Stats:
    def __init__(self):
        self.total_messages = 0
        self.instruments = set()
        self.messages_per_instrument = defaultdict(int)
        self.currency_count = defaultdict(int)  # BTC vs ETH
        self.start_time = None
        self.first_msg_time = None
        self.last_msg_time = None
        # 采样数据
        self.sample_greeks = {}

    def record(self, topic: str, payload: bytes):
        now = time.time()
        if self.first_msg_time is None:
            self.first_msg_time = now

        self.total_messages += 1
        self.last_msg_time = now

        instrument = topic.split("/")[-1]
        self.instruments.add(instrument)
        self.messages_per_instrument[instrument] += 1

        # 按币种统计
        if instrument.startswith("BTC"):
            self.currency_count["BTC"] += 1
        elif instrument.startswith("ETH"):
            self.currency_count["ETH"] += 1
        else:
            self.currency_count["OTHER"] += 1

        # 采样: 记录每个币种最新一条 greeks 数据
        try:
            data = json.loads(payload)
            currency = "BTC" if instrument.startswith("BTC") else "ETH"
            if currency not in self.sample_greeks and data.get("greeks"):
                self.sample_greeks[currency] = {
                    "instrument": instrument,
                    "mark_price": data.get("mark_price"),
                    "mark_iv": data.get("mark_iv"),
                    "greeks": data["greeks"],
                }
        except (json.JSONDecodeError, KeyError):
            pass


def main():
    parser = argparse.ArgumentParser(description="Deribit Option Ticker Monitor (MQTT-WS)")
    parser.add_argument("--host", default="192.168.2.86", help="Higress gateway IP")
    parser.add_argument("--port", type=int, default=31693, help="Higress HTTP NodePort")
    parser.add_argument("--path", default="/mqtt", help="MQTT-WS path")
    parser.add_argument("--topic", default="t/deribit/option_ticker/#", help="MQTT topic filter")
    parser.add_argument("--duration", type=int, default=60, help="监听时长(秒)")
    args = parser.parse_args()

    stats = Stats()
    connected = False

    def on_connect(client, userdata, flags, rc, props=None):
        nonlocal connected
        if rc == 0:
            connected = True
            print(f"✓ Connected to ws://{args.host}:{args.port}{args.path}")
            client.subscribe(args.topic, qos=0)
            print(f"✓ Subscribed to: {args.topic}")
            print(f"✓ Listening for {args.duration}s...\n")
            stats.start_time = time.time()
        else:
            print(f"✗ Connection failed: rc={rc}")
            sys.exit(1)

    def on_message(client, userdata, msg):
        stats.record(msg.topic, msg.payload)

    def on_disconnect(client, userdata, flags, rc, props=None):
        if rc != 0:
            print(f"✗ Unexpected disconnect: rc={rc}")

    # Setup client
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"ws-monitor-{int(time.time())}",
        transport="websockets",
    )
    client.ws_set_options(path=args.path)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Graceful shutdown
    def shutdown(sig, frame):
        print("\n⏹ Interrupted")
        client.loop_stop()
        print_report(stats, args.duration)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Connect
    print(f"Connecting to ws://{args.host}:{args.port}{args.path} ...")
    try:
        client.connect(args.host, args.port, keepalive=60)
    except Exception as e:
        print(f"✗ Connection error: {e}")
        sys.exit(1)

    client.loop_start()

    # Wait for duration
    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        pass

    client.loop_stop()
    client.disconnect()

    # Print report
    print_report(stats, args.duration)


def print_report(stats: Stats, duration: int):
    elapsed = (stats.last_msg_time - stats.start_time) if stats.start_time and stats.last_msg_time else 0

    print("=" * 60)
    print("  📊 Deribit Option Monitor — 统计报告")
    print("=" * 60)

    if stats.total_messages == 0:
        print("\n  ⚠ 未收到任何消息")
        return

    msg_per_sec = stats.total_messages / elapsed if elapsed > 0 else 0

    print(f"\n  📡 消息统计:")
    print(f"     总消息数:        {stats.total_messages:,}")
    print(f"     不同期权合约数:  {len(stats.instruments):,}")
    print(f"     监听时长:        {elapsed:.1f}s / {duration}s")
    print(f"     消息速率:        {msg_per_sec:.1f} msg/s")

    print(f"\n  💱 币种分布:")
    for currency, count in sorted(stats.currency_count.items(), key=lambda x: -x[1]):
        pct = count / stats.total_messages * 100
        print(f"     {currency:6s}: {count:>8,} ({pct:.1f}%)")

    # Top 5 most active instruments
    top5 = sorted(stats.messages_per_instrument.items(), key=lambda x: -x[1])[:5]
    print(f"\n  🔥 最活跃期权 (Top 5):")
    for inst, count in top5:
        print(f"     {inst:30s} {count:>5} msgs")

    # Sample greeks data
    if stats.sample_greeks:
        print(f"\n  📈 样本数据:")
        for currency, sample in stats.sample_greeks.items():
            g = sample["greeks"]
            print(f"     [{currency}] {sample['instrument']}")
            print(f"       mark_price={sample['mark_price']}, mark_iv={sample['mark_iv']}")
            print(f"       Δ={g.get('delta', 0):.4f}  Γ={g.get('gamma', 0):.6f}  "
                  f"Θ={g.get('theta', 0):.4f}  ν={g.get('vega', 0):.4f}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
