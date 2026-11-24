DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pricing_reader') THEN
        CREATE ROLE pricing_reader LOGIN PASSWORD 'reader_dev';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE pricing TO pricing_reader;
GRANT USAGE ON SCHEMA public TO pricing_reader;
GRANT SELECT ON precos_diarios, precos_historico TO pricing_reader;
