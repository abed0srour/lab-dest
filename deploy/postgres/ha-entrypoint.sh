#!/bin/sh
# PostgreSQL with automatic failover between two nodes: this one and $PEER.
#  - If the peer is already the primary, this node (re)joins as its standby, by cloning it with
#    pg_basebackup. It is never started as a second primary, even if it was the primary before.
#  - On the very first start, the node with INITIAL_ROLE=primary creates the database.
#  - While this node is a standby, a watchdog promotes it to primary when the primary has been
#    unreachable for 10 s.
# Clients list both hosts with target_session_attrs=read-write, so they always find the primary.
set -eu
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
export PGPASSWORD="$POSTGRES_PASSWORD"
log() { echo "[ha $(hostname)] $*"; }

peer_is_primary() {
    [ "$(psql -h "$PEER" -U "$POSTGRES_USER" -d postgres -Atc 'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" = "t" ]
}

follow_peer() {
    log "cloning $PEER and following it as a standby"
    mkdir -p "$PGDATA"
    find "$PGDATA" -mindepth 1 -delete
    chown postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
    gosu postgres pg_basebackup -h "$PEER" -U "$POSTGRES_USER" -D "$PGDATA" -X stream -c fast -R
    echo "primary_conninfo = 'host=$PEER port=5432 user=$POSTGRES_USER password=$POSTGRES_PASSWORD application_name=$(hostname)'" \
        >> "$PGDATA/postgresql.auto.conf"
}

if peer_is_primary; then
    if [ -f "$PGDATA/standby.signal" ]; then log "starting as the standby of $PEER"; else follow_peer; fi
elif [ ! -s "$PGDATA/PG_VERSION" ]; then
    if [ "${INITIAL_ROLE:-standby}" = primary ]; then
        log "first start: creating the database as the primary"
    else
        log "waiting for $PEER to become the primary"
        until peer_is_primary; do sleep 2; done
        follow_peer
    fi
elif [ -f "$PGDATA/standby.signal" ]; then
    log "starting as a standby; $PEER is not the primary right now"
else
    log "starting as the primary"
fi

watchdog() {
    # Time-based, not a count of checks: once the peer's container is gone, looking up its name can
    # itself take seconds, so every check is capped at 2 s and the decision uses the clock.
    last_seen=$(date +%s)
    while sleep 1; do
        if [ ! -f "$PGDATA/standby.signal" ]; then last_seen=$(date +%s); continue; fi
        if timeout 2 pg_isready -h "$PEER" -q -t 1; then last_seen=$(date +%s); fi
        if [ $(( $(date +%s) - last_seen )) -ge 10 ]; then
            log "primary $PEER unreachable for 10 s: promoting this standby to primary"
            psql -h 127.0.0.1 -U "$POSTGRES_USER" -d postgres -Atc 'SELECT pg_promote()' || true
            last_seen=$(date +%s)
        fi
    done
}
watchdog &

exec docker-entrypoint.sh postgres -c wal_keep_size=512MB -c hot_standby=on
