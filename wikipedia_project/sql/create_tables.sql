CREATE TABLE IF NOT EXISTS wikipedia_pageviews (
    id SERIAL PRIMARY KEY,
    company VARCHAR(100) NOT NULL,
    page_title VARCHAR(255) NOT NULL,
    view_count INTEGER NOT NULL,
    domain VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate entries for the same company and hour
    UNIQUE (company, execution_date)
);

-- ---------------------------------------------------------
-- Indexes for query performance
-- ---------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_pageviews_company
    ON wikipedia_pageviews (company);

CREATE INDEX IF NOT EXISTS idx_pageviews_execution_date
    ON wikipedia_pageviews (execution_date);

CREATE INDEX IF NOT EXISTS idx_pageviews_view_count
    ON wikipedia_pageviews (view_count);

-- ---------------------------------------------------------
-- Table and column documentation
-- ---------------------------------------------------------

COMMENT ON TABLE wikipedia_pageviews IS
    'Stores hourly Wikipedia pageview counts for tracked companies';

COMMENT ON COLUMN wikipedia_pageviews.company IS
    'Company name (e.g. Amazon, Apple, Facebook, Google, Microsoft)';

COMMENT ON COLUMN wikipedia_pageviews.page_title IS
    'Wikipedia page title associated with the company';

COMMENT ON COLUMN wikipedia_pageviews.view_count IS
    'Number of pageviews recorded during the hour';

COMMENT ON COLUMN wikipedia_pageviews.execution_date IS
    'Date and hour when the pageviews were collected';