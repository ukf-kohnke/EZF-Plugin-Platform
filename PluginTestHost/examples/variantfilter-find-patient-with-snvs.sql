-- Find patient IDs suitable for scenario-variantfilter-load-snv-data.json.
-- The query mirrors VariantFilter's current analysis selection (TOP 1 ordered by a.id)
-- and keeps only patients whose selected analysis contains SNV variants.
WITH selected_analysis AS (
    SELECT
        p.id AS patient_id,
        (
            SELECT TOP (1) a.id
            FROM samples s
            INNER JOIN sequences seq ON seq.sample_id = s.id
            INNER JOIN seq_ana_maps sam ON sam.sequence_id = seq.id
            INNER JOIN analyses a ON a.id = sam.analysis_id
            WHERE s.patient_id = p.id
            ORDER BY a.id
        ) AS analysis_id
    FROM patients p
)
SELECT TOP (20)
    CONVERT(varchar(36), sa.patient_id) AS patient_id,
    COUNT(DISTINCT sv.id) AS simple_variant_count
FROM selected_analysis sa
INNER JOIN analysis_specific_variants asv ON asv.analysis_id = sa.analysis_id
INNER JOIN simple_variants sv ON sv.variant_id = asv.variant_id
WHERE sa.analysis_id IS NOT NULL
GROUP BY sa.patient_id
HAVING COUNT(DISTINCT sv.id) > 0
ORDER BY simple_variant_count DESC, sa.patient_id;
