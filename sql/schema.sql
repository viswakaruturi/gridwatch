CREATE TABLE electricity (
    period TIMESTAMPTZ NOT NULL,
    respondent VARCHAR(255) NOT NULL,
    respondent_name VARCHAR(255),
    type VARCHAR(255) NOT NULL,
    type_name VARCHAR(255),
    value NUMERIC,
    value_units VARCHAR(255),

    PRIMARY KEY (period, respondent, type)
);