from __future__ import annotations

import json

import aio_pika


class Broker:
    async def connect(self, url: str):
        self.connection = await aio_pika.connect_robust(url, timeout=20)
        self.channel = await self.connection.channel(publisher_confirms=True)
        await self.channel.set_qos(prefetch_count=1)
        # Quorum queue, durable messages, publisher confirms and manual acknowledgements.
        # One RabbitMQ node provides persistence, not high availability.
        self.dead = await self.channel.declare_queue(
            "retention.dead", durable=True, arguments={"x-queue-type": "quorum"}
        )
        self.queue = await self.channel.declare_queue(
            "retention.jobs",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": "retention.dead",
                "x-delivery-limit": 20,
            },
        )
        return self

    async def publish(self, kind: str, key: str):
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({"v": 1, "kind": kind, "key": key}).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=kind + ":" + key,
            ),
            routing_key=self.queue.name,
            mandatory=True,
        )

    async def close(self):
        await self.connection.close()
