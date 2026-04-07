# pgwire

Implements the PostgreSQL v3 frontend/backend wire protocol, allowing standard PostgreSQL clients (`psql`, `pg_dump`, JDBC, libpq, etc.) to connect to LolaDB.

Handles the full protocol flow: SSL negotiation, startup/authentication, simple and extended query protocols (Parse/Bind/Describe/Execute), parameter status reporting, error/notice messaging, and graceful termination.

Also provides catalog metadata responses for `pg_dump` compatibility.
