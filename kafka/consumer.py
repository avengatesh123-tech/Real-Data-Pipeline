import json

import pyarrow as pa
import pyarrow.parquet as pq

from confluent_kafka import Consumer

consumer = Consumer({

    "bootstrap.servers":"localhost:9092",

    "group.id":"orders-group_v2",

    "auto.offset.reset":"earliest",

    "enable.auto.commit":False

})

consumer.subscribe(["orders"])

buffer=[]

writer=None

BATCH_SIZE=10

PARQUET_FILE="orders.parquet"

print("Waiting for messages...")

try:

    while True:

        msg=consumer.poll(1)

        if msg is None:
            continue

        if msg.error():

            print(msg.error())

            continue

        order=json.loads(msg.value().decode())

        print(order)

        buffer.append(order)

        if len(buffer)>=BATCH_SIZE:

            table=pa.Table.from_pylist(buffer)

            if writer is None:

                writer=pq.ParquetWriter(
                    PARQUET_FILE,
                    table.schema
                )

            writer.write_table(table)

            buffer.clear()

        consumer.commit(asynchronous=False)

except KeyboardInterrupt:

    print("Stopping Consumer...")

finally:

    if buffer:

        table=pa.Table.from_pylist(buffer)

        if writer is None:

            writer=pq.ParquetWriter(
                PARQUET_FILE,
                table.schema
            )

        writer.write_table(table)

    if writer:

        writer.close()

    consumer.close()

    print("Consumer Closed")
