#!/usr/bin/env python3
"""
Test psycopg with CockroachDB.
"""

import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter

import psycopg2
from psycopg2.errors import SerializationFailure

def create_store(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS store (pid INT PRIMARY KEY, quantity INT, pname VARCHAR)"
        )
        cur.execute("UPSERT INTO store (pid, quantity,pname) VALUES (0,0,'demo')")
        logging.debug("create_store(): status message: %s", cur.statusmessage)
    conn.commit()

def add_item(conn,id,quantity,name):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO store VALUES (%s,%s,%s)",(id,quantity,name))
        logging.debug("add_item(): status message: %s", cur.statusmessage)
    conn.commit()

def print_store(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pid, quantity,pname FROM store")
        logging.debug("print_store(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        print(f"Items available at {time.asctime()}:")
        for row in rows:
            print(row)

def del_prod(conn,id):
    with conn.cursor() as cur:
        cur.execute("DELETE from STORE WHERE pid= %s",(id,))
    conn.commit()
    logging.debug("del_prod(): status message: %s", cur.statusmessage)
        
def buy_items(conn, id, quantity):
    with conn.cursor() as cur:

        # Check the current balance.
        cur.execute("SELECT quantity FROM store WHERE pid = %s", (id,))
        from_balance = cur.fetchone()[0]
        if from_balance < quantity:
            raise RuntimeError(
                f"Insufficient quantity"
            )

        cur.execute(
            "UPDATE store SET quantity = quantity - %s WHERE pid = %s", (quantity, id)
        )
    conn.commit()
    logging.debug("buy_items(): status message: %s", cur.statusmessage)

def main():
    opt = parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    create_store(conn)
    while True:
        print("Enter choice:")
        print("1. Add items in shop")
        print("2. Buy item")
        print("3. Remove item")
        print("4. View Items available")
        print("5. Exit")
        inp = int(input())
        print("Current table is:")
        print_store(conn)
        if inp==1:
            print("Enter item id:")
            id=int(input())
            print("Enter quantity:")
            quantity=int(input())
            print("Enter name:")
            name=str(input())
            add_item(conn,id,quantity,name)
        elif inp==2:
            print("Id of item to buy:")
            id=int(input())
            print("Enter quantity:")
            quantity=int(input())
            buy_items(conn,id,quantity)
        elif inp==3:
            print("Enter product id to delete")
            id=int(input())
            del_prod(conn,id)
        elif inp==4:
            pass
        else:
            break
    print_store(conn)
    conn.close()


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "dsn",
        help="""\
database connection string

For cockroach demo, use
'postgresql://<username>:<password>@<hostname>:<port>/bank?sslmode=require',
with the username and password created in the demo cluster, and the hostname
and port listed in the (sql/tcp) connection parameters of the demo cluster
welcome message.

For CockroachCloud Free, use
'postgres://<username>:<password>@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/<cluster-name>.bank?sslmode=verify-full&sslrootcert=<your_certs_directory>/cc-ca.crt'.

If you are using the connection string copied from the Console, your username,
password, and cluster name will be pre-populated. Replace
<your_certs_directory> with the path to the 'cc-ca.crt' downloaded from the
Console.

"""
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":
    main()
