-- Esquema de la base de datos única (PostgreSQL).
-- Se ejecuta una sola vez, al crear el volumen, por el contenedor postgres.
-- Así las 9 réplicas no compiten por crear las tablas al arrancar.

CREATE TABLE IF NOT EXISTS orders (
    id         SERIAL PRIMARY KEY,
    symbol     TEXT             NOT NULL,
    side       TEXT             NOT NULL,
    price      DOUBLE PRECISION NOT NULL,
    amount     DOUBLE PRECISION NOT NULL,
    status     TEXT             NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ      NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
    id         SERIAL PRIMARY KEY,
    symbol     TEXT             NOT NULL UNIQUE,
    price      DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ      NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    name       TEXT             NOT NULL,
    email      TEXT             NOT NULL UNIQUE,
    balance    DOUBLE PRECISION NOT NULL DEFAULT 10000.0,
    created_at TIMESTAMPTZ      NOT NULL
);

-- Datos semilla
INSERT INTO prices (symbol, price, updated_at) VALUES
    ('BTC/USD', 65000.0, now()),
    ('ETH/USD',  3500.0, now()),
    ('SOL/USD',   180.0, now())
ON CONFLICT (symbol) DO NOTHING;

INSERT INTO users (name, email, balance, created_at)
VALUES ('Trader Demo', 'demo@mercado.com', 10000.0, now())
ON CONFLICT (email) DO NOTHING;
