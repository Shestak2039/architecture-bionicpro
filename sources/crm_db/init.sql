CREATE TABLE clients (
    client_id   SERIAL PRIMARY KEY,
    user_login  VARCHAR(64)  NOT NULL UNIQUE,
    full_name   VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    country     VARCHAR(64)  NOT NULL
);

CREATE TABLE prostheses (
    prosthesis_id VARCHAR(32)  PRIMARY KEY,
    client_id     INT          NOT NULL REFERENCES clients (client_id),
    model         VARCHAR(128) NOT NULL,
    purchase_date DATE         NOT NULL
);

INSERT INTO clients (user_login, full_name, email, country) VALUES
    ('prothetic1', 'Prothetic One',   'prothetic1@example.com', 'Россия'),
    ('prothetic2', 'Prothetic Two',   'prothetic2@example.com', 'Россия'),
    ('prothetic3', 'Prothetic Three', 'prothetic3@example.com', 'Россия');

INSERT INTO prostheses (prosthesis_id, client_id, model, purchase_date) VALUES
    ('PR-1001', 1, 'BionicPRO Hand X2', '2025-11-10'),
    ('PR-1002', 1, 'BionicPRO Arm A1',  '2026-03-02'),
    ('PR-2001', 2, 'BionicPRO Hand X2', '2026-01-20'),
    ('PR-3001', 3, 'BionicPRO Leg L3',  '2026-05-15');
