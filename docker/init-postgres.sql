CREATE ROLE pricing LOGIN PASSWORD 'pricing_dev';
CREATE DATABASE pricing OWNER pricing;
\connect pricing
SET ROLE pricing;
CREATE TABLE precos_diarios (
    data_referencia date NOT NULL,
    categoria text NOT NULL,
    quantidade integer NOT NULL CHECK (quantidade > 0),
    preco_medio numeric NOT NULL,
    preco_minimo numeric NOT NULL,
    preco_maximo numeric NOT NULL,
    PRIMARY KEY (data_referencia, categoria)
);
CREATE TABLE precos_historico (
    run_id text NOT NULL,
    data_execucao timestamptz NOT NULL,
    data_referencia date NOT NULL,
    categoria text NOT NULL,
    quantidade integer NOT NULL CHECK (quantidade > 0),
    preco_medio numeric NOT NULL,
    preco_minimo numeric NOT NULL,
    preco_maximo numeric NOT NULL,
    PRIMARY KEY (run_id, categoria)
);
