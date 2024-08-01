WITH to_update AS (
    SELECT id
    FROM observations
    WHERE internal_model = 'LANGFUSETMPNOMODEL'
    AND "type" = 'GENERATION'
    LIMIT 50000
)
UPDATE "observations"
SET "internal_model" = NULL
WHERE id IN (SELECT id FROM to_update)
RETURNING id;