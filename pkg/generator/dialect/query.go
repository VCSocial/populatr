package dialect

const findAllColumnsOfTable = `
SELECT
	c.column_name as column_name,
    c.udt_name as udt_name,
    c.character_maximum_length as character_maximum_legnth,
    c.numeric_precision as numeric_precision,
	c.datetime_precision as datetime_precision,
    c.is_nullable as is_nullable,
	c.column_default as column_default,
    CASE
		WHEN kt.constraint_type IS NOT NULL
        	THEN kt.constraint_type
        ELSE 'BASIC KEY'
    END as constraint_type
FROM information_schema.columns c
LEFT JOIN (
  		SELECT
  			tc.constraint_type,
  			tc.table_name,
  			kcu.column_name
  		FROM information_schema.table_constraints tc
  		JOIN information_schema.key_column_usage kcu
  			ON tc.constraint_name = kcu.constraint_name
	) kt ON c.column_name = kt.column_name
		AND c.table_name = kt.table_name
WHERE c.table_name = $1;
`

const findAllTableRelations = `
SELECT
    tc2.table_name as parent_table,
    kcu2.column_name as parent_column,
    tc.table_name as child_table,
    kcu.column_name as child_column
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.referential_constraints rc
	ON tc.constraint_name = rc.constraint_name
    AND tc.constraint_schema = rc.constraint_schema
    AND tc.constraint_catalog = rc.constraint_catalog
LEFT JOIN information_schema.table_constraints tc2
	ON rc.unique_constraint_name = tc2.constraint_name
    AND rc.constraint_schema = tc2.constraint_schema
    AND rc.constraint_catalog = tc2.constraint_catalog
LEFT JOIN information_schema.key_column_usage kcu
	ON tc.constraint_name = kcu.constraint_name
    AND tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_catalog = kcu.constraint_catalog
LEFT JOIN information_schema.key_column_usage kcu2
	ON tc2.constraint_name = kcu2.constraint_name
    AND tc2.constraint_schema = kcu2.constraint_schema
    AND tc2.constraint_catalog = kcu2.constraint_catalog
ORDER BY parent_table
`

const insertQueryTemplate = "INSERT INTO %s (%s) VALUES %s"
