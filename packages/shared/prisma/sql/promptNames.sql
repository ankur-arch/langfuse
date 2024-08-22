SELECT 
    p.name promptName, 
    count(*)::int AS count
FROM 
    prompts p
JOIN 
    observations o 
    ON o.prompt_id = p.id
WHERE 
    o.type = 'GENERATION'
    AND o.project_id = $1
    AND o.prompt_id IS NOT NULL
    AND p.project_id = $1
GROUP BY 
    p.name
LIMIT 1000;