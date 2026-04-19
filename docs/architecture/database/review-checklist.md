# Review Checklist

Use this checklist when deciding whether a table, column, or index still deserves to exist.

## Table-Level Questions

- What is the grain of one row?
- Is this table authoritative, or can it be derived from another table?
- Is it on the hot write path, hot read path, both, or neither?
- Does it store the latest state only, or does it need history?
- If the table disappeared tomorrow, which CLI stages would break?

## Column-Level Questions

- Is the column read by production code, or only carried around because the provider exposed it?
- Is the type narrower than it could be?
- Should the column be nullable?
- Is the column duplicated in another table?
- Is the column part of a key only because of historical convenience?
- Does a wide text/blob field belong in the hot table, or should it be isolated?

## Key And Relationship Questions

- Does the primary key match the most common lookup pattern?
- Would a surrogate key or natural key be faster or simpler?
- Are logical foreign keys documented even when SQLite does not enforce them?
- Could orphan rows exist today because the relationship is application-managed?

## Index Questions

- Which exact query uses this index?
- Does the index column order match `WHERE`, `JOIN`, and `ORDER BY` patterns?
- Is the index redundant with the primary key or another composite index?
- Is the index selective enough to justify its write overhead?
- If the index disappeared, which command would slow down first?

## Performance Questions

- Does the table force repeated scans during bulk workflows?
- Are the hottest queries reading only the columns they need?
- Is the table wider than necessary for its hottest query path?
- Are there state tables that duplicate each other without reducing runtime enough to justify the storage and code complexity?
- Are there write-amplification costs from indexes that the pipeline barely uses?
