#!/bin/sh
# Runs once, when the primary creates the database: allow standbys to stream the WAL from it.
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
