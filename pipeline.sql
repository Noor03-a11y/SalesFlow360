BEGIN -- Declare variables at the start of the script
DECLARE job_id_1 STRING;

DECLARE job_id_2 STRING;

DECLARE job_id_3 STRING;

-- Create a temporary table to track job progress
CREATE TEMP TABLE job_tracker (
    job_id STRING,
    step STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- Query 1: Insert Partner Leads and Non-Partner Leads
SET
    job_id_1 = GENERATE_UUID ();

INSERT INTO
    job_tracker (job_id, step, start_time)
VALUES
    (job_id_1, 'Query1', CURRENT_TIMESTAMP());

-- Query 1: Insert Partner Leads
INSERT INTO
    `superb-vigil-419105.Sales.partner_leads` (
        prtnr_lead_id,
        prtnr_lead_timestamp,
        prtnr_lead_company_name,
        prtnr_lead_name,
        prtnr_lead_contact_no,
        prtnr_lead_mail,
        prtnr_lead_product,
        prtnr_lead_descrptn,
        prtnr_lead_region,
        prtnr_lead_nbd_type,
        prtnr_lead_source,
        prtnr_lead_sc_assigned,
        prtnr_lead_salesperson,
        prtnr_lead_country,
        prtnr_lead_city,
        prtnr_lead_state,
        prtnr_lead_industry,
        prtnr_lead_customer_type
    )
SELECT DISTINCT
    mst.mst_lead_id,
    mst.mst_lead_timestamp,
    mst.mst_lead_company_name,
    mst.mst_lead_name,
    mst.mst_lead_contact_no,
    mst.mst_lead_mail,
    mst.mst_lead_product,
    mst.mst_lead_descrptn,
    mst.mst_lead_region,
    mst.mst_lead_nbd_type,
    mst.mst_lead_source,
    mst.mst_lead_sc_assigned,
    mst.mst_lead_salesperson,
    mst.mst_lead_country,
    mst.mst_lead_city,
    mst.mst_lead_state,
    mst.mst_lead_industry,
    mst.mst_lead_customer_type
FROM
    `superb-vigil-419105.Sales.mst_leads` mst
WHERE
    mst.mst_lead_company_name IN (
        'Advanced Sealing Technology B.V.',
        'EXACTSEAL INC.',
        'VILITEK LLC',
        'AMERICAN RUBBER CORPORATION',
        'Exactsilicone Inc'
    )
    AND mst.mst_lead_timestamp IS NOT NULL
    AND NOT EXISTS (
        SELECT
            1
        FROM
            `superb-vigil-419105.Sales.partner_leads` pl
        WHERE
            pl.prtnr_lead_id = mst.mst_lead_id
            AND pl.prtnr_lead_timestamp = mst.mst_lead_timestamp
    );

-- Query 2: Insert Non-Partner Leads
INSERT INTO
    `superb-vigil-419105.Sales.non_partner_leads` (
        np_lead_id,
        np_lead_timestamp,
        np_lead_company_name,
        np_lead_name,
        np_lead_contact_no,
        np_lead_mail,
        np_lead_product,
        np_lead_descrptn,
        np_lead_region,
        np_lead_nbd_type,
        np_lead_source,
        np_lead_sc_assigned,
        np_lead_salesperson,
        np_lead_country,
        np_lead_city,
        np_lead_state,
        np_lead_industry,
        np_lead_customer_type
    )
SELECT DISTINCT
    mst.mst_lead_id,
    mst.mst_lead_timestamp,
    mst.mst_lead_company_name,
    mst.mst_lead_name,
    mst.mst_lead_contact_no,
    mst.mst_lead_mail,
    mst.mst_lead_product,
    mst.mst_lead_descrptn,
    mst.mst_lead_region,
    mst.mst_lead_nbd_type,
    mst.mst_lead_source,
    mst.mst_lead_sc_assigned,
    mst.mst_lead_salesperson,
    mst.mst_lead_country,
    mst.mst_lead_city,
    mst.mst_lead_state,
    mst.mst_lead_industry,
    mst.mst_lead_customer_type
FROM
    `superb-vigil-419105.Sales.mst_leads` mst
WHERE
    mst.mst_lead_company_name NOT IN (
        'Advanced Sealing Technology B.V.',
        'EXACTSEAL INC.',
        'VILITEK LLC',
        'AMERICAN RUBBER CORPORATION',
        'Exactsilicone Inc'
    )
    AND mst.mst_lead_timestamp IS NOT NULL
    AND NOT EXISTS (
        SELECT
            1
        FROM
            `superb-vigil-419105.Sales.non_partner_leads` npl
        WHERE
            npl.np_lead_id = mst.mst_lead_id
            AND npl.np_lead_timestamp = mst.mst_lead_timestamp
    );

-- Query 3: Merge into latest_np_responses
MERGE INTO `superb-vigil-419105.Sales.latest_np_responses` AS target USING (
    SELECT
        *
    FROM
        (
            SELECT
                np_data_lead_ID,
                np_data_step_no,
                latest_timestamp,
                latest_status,
                latest_unqualified_reason
            FROM
                (
                    SELECT
                        np_data_lead_ID,
                        np_data_step_no,
                        np_data_lead_timestamp AS latest_timestamp,
                        np_data_lead_status AS latest_status,
                        np_data_unqualified_reason AS latest_unqualified_reason,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                np_data_lead_ID,
                                np_data_step_no
                            ORDER BY
                                np_data_lead_timestamp DESC
                        ) AS rn
                    FROM
                        `superb-vigil-419105.Sales.np_responses_data`
                    WHERE
                        np_data_step_no IN ('LC1', 'LC3', 'LC4')
                )
            WHERE
                rn = 1
            UNION ALL
            SELECT
                np_st2_data_lead_ID AS np_data_lead_ID,
                'LC2' AS np_data_step_no,
                np_st2_data_lead_timestamp AS latest_timestamp,
                np_st2_data_lead_status AS latest_status,
                np_st2_data_unqualified_reason AS latest_unqualified_reason
            FROM
                (
                    SELECT
                        np_st2_data_lead_ID,
                        np_st2_data_lead_timestamp,
                        np_st2_data_lead_status,
                        np_st2_data_unqualified_reason,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                np_st2_data_lead_ID
                            ORDER BY
                                np_st2_data_lead_timestamp DESC
                        ) AS rn
                    FROM
                        `superb-vigil-419105.Sales.np_responses_data_st2`
                )
            WHERE
                rn = 1
        )
) AS SOURCE ON target.np_data_lead_ID = source.np_data_lead_ID
AND target.np_data_step_no = source.np_data_step_no WHEN MATCHED
AND (
    target.latest_timestamp < source.latest_timestamp
    OR target.latest_status != source.latest_status
    OR IFNULL (target.latest_unqualified_reason, '') != IFNULL (source.latest_unqualified_reason, '')
) THEN
UPDATE
SET
    latest_timestamp = source.latest_timestamp,
    latest_status = source.latest_status,
    latest_unqualified_reason = source.latest_unqualified_reason WHEN NOT MATCHED THEN INSERT (
        np_data_lead_ID,
        np_data_step_no,
        latest_timestamp,
        latest_status,
        latest_unqualified_reason
    )
VALUES
    (
        source.np_data_lead_ID,
        source.np_data_step_no,
        source.latest_timestamp,
        source.latest_status,
        source.latest_unqualified_reason
    );

-- Query 4: Update non_partner_leads
UPDATE `superb-vigil-419105.Sales.non_partner_leads` AS np
SET
    np_st1_act_dte = CASE
        WHEN np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN COALESCE(
            np.np_st1_act_dte,
            combined_data.lc1_latest_timestamp
        )
        ELSE np.np_st1_act_dte
    END,
    np_st1_status = CASE
        WHEN np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN COALESCE(np.np_st1_status, combined_data.lc1_latest_status)
        ELSE np.np_st1_status
    END,
    np_st1_unqualified_reason = CASE
        WHEN np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN COALESCE(
            np.np_st1_unqualified_reason,
            combined_data.lc1_latest_unqualified_reason
        )
        ELSE np.np_st1_unqualified_reason
    END,
    np_st2_act_dte = CASE
        WHEN np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN COALESCE(
            np.np_st2_act_dte,
            combined_data.lc2_latest_timestamp
        )
        ELSE np.np_st2_act_dte
    END,
    np_st2_status = CASE
        WHEN np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN COALESCE(np.np_st2_status, combined_data.lc2_latest_status)
        ELSE np.np_st2_status
    END,
    np_st2_unqualified_reason = CASE
        WHEN np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN COALESCE(
            np.np_st2_unqualified_reason,
            combined_data.lc2_latest_unqualified_reason
        )
        ELSE np.np_st2_unqualified_reason
    END,
    np_st3_act_dte = COALESCE(
        np.np_st3_act_dte,
        combined_data.lc3_latest_timestamp
    ),
    np_st3_status = COALESCE(np.np_st3_status, combined_data.lc3_latest_status),
    np_st3_unqualified_reason = COALESCE(
        np.np_st3_unqualified_reason,
        combined_data.lc3_latest_unqualified_reason
    ),
    np_st4_act_dte = COALESCE(
        np.np_st4_act_dte,
        combined_data.lc4_latest_timestamp
    ),
    np_st4_status = COALESCE(np.np_st4_status, combined_data.lc4_latest_status),
    np_st4_unqualified_reason = COALESCE(
        np.np_st4_unqualified_reason,
        combined_data.lc4_latest_unqualified_reason
    )
FROM
    (
        SELECT
            np_data_lead_ID,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC1' THEN latest_timestamp
                END
            ) AS lc1_latest_timestamp,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC1' THEN latest_status
                END
            ) AS lc1_latest_status,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC1' THEN latest_unqualified_reason
                END
            ) AS lc1_latest_unqualified_reason,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC2' THEN latest_timestamp
                END
            ) AS lc2_latest_timestamp,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC2' THEN latest_status
                END
            ) AS lc2_latest_status,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC2' THEN latest_unqualified_reason
                END
            ) AS lc2_latest_unqualified_reason,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC3' THEN latest_timestamp
                END
            ) AS lc3_latest_timestamp,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC3' THEN latest_status
                END
            ) AS lc3_latest_status,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC3' THEN latest_unqualified_reason
                END
            ) AS lc3_latest_unqualified_reason,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC4' THEN latest_timestamp
                END
            ) AS lc4_latest_timestamp,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC4' THEN latest_status
                END
            ) AS lc4_latest_status,
            MAX(
                CASE
                    WHEN np_data_step_no = 'LC4' THEN latest_unqualified_reason
                END
            ) AS lc4_latest_unqualified_reason
        FROM
            `superb-vigil-419105.Sales.latest_np_responses`
        WHERE
            np_data_step_no IN ('LC1', 'LC2', 'LC3', 'LC4')
        GROUP BY
            np_data_lead_ID
    ) AS combined_data
WHERE
    np.np_lead_id = combined_data.np_data_lead_ID
    AND (
        (
            np.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND (
                np.np_st1_act_dte IS NULL
                OR np.np_st2_act_dte IS NULL
                OR np.np_st3_act_dte IS NULL
                OR np.np_st4_act_dte IS NULL
            )
        )
    OR (
            np.np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
            AND (
                np.np_st3_act_dte IS NULL
                OR np.np_st4_act_dte IS NULL
            )
        

        )
    OR (
            np.np_lead_nbd_type IN ('CRR-NG')
            AND (
                 np.np_st4_act_dte IS NULL
            )
        

        )
    );

--QUERY4
UPDATE job_tracker
SET
    end_time = CURRENT_TIMESTAMP()
WHERE
    job_id = job_id_1;

-- Wait for Query 1 to complete
CALL `superb-vigil-419105.Sales.wait_for_job` (job_id_1);

-- Query 2: Update planned dates in non_partner_leads
SET
    job_id_2 = GENERATE_UUID ();

INSERT INTO
    job_tracker (job_id, step, start_time)
VALUES
    (job_id_2, 'Query2', CURRENT_TIMESTAMP());

BEGIN -- Query 1
UPDATE `superb-vigil-419105.Sales.non_partner_leads`
SET
    np_final_status = CASE
        WHEN np_lead_id IS NOT NULL THEN CASE -- Case 1
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_pln_dte IS NOT NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(np_st1_status, 'No Status Updated') -- Case 2
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st2_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NOT NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(np_st2_status, np_st1_status, 'Error') -- Case 3
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st3_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(
                np_st3_status,
                np_st2_status,
                np_st1_status,
                'No Status Updated'
            ) ----- Case 4
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st4_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st3_pln_dte IS NOT NULL THEN COALESCE(np_st4_status, np_st3_status, 'error') -- Case 5
            WHEN np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
            AND np_st3_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(np_st3_status, 'No Status Updated') ---------Case 6
            WHEN np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
            AND np_st4_pln_dte IS NOT NULL
            AND np_st3_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NULL
            AND np_st2_pln_dte IS NULL THEN COALESCE(np_st4_status, np_st3_status, 'No Status Updated') -- Case 7
            WHEN np_lead_nbd_type = 'CRR-NG'
            AND np_st4_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL THEN COALESCE(np_st4_status, 'Completed')
            ELSE np_final_status -- Keep the existing value if none of the conditions are met
        END
        ELSE np_final_status -- Keep the existing value if np_lead_id is NULL
    END
WHERE
    np_lead_id IS NOT NULL;
UPDATE `superb-vigil-419105.Sales.non_partner_leads` main_table
SET
    np_st1_pln_dte = CASE
        WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') THEN TIMESTAMP_ADD (
            TIMESTAMP_ADD (
                subquery.first_valid_date,
                INTERVAL EXTRACT(
                    HOUR
                    FROM
                        main_table.np_lead_timestamp
                ) HOUR
            ),
            INTERVAL EXTRACT(
                MINUTE
                FROM
                    main_table.np_lead_timestamp
            ) MINUTE
        )
        ELSE NULL
    END
FROM
    (
        SELECT
            np_lead_id,
            MIN(date) AS first_valid_date
        FROM
            (
                SELECT
                    np_lead_id,
                    np_lead_timestamp,
                    TIMESTAMP_ADD (
                        TIMESTAMP_TRUNC (np_lead_timestamp, DAY),
                        INTERVAL n DAY
                    ) AS date
                FROM
                    `superb-vigil-419105.Sales.non_partner_leads`
                    CROSS JOIN UNNEST (GENERATE_ARRAY (0, 30)) AS n
                WHERE
                    np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                    AND np_st1_pln_dte IS NULL
            ) date_series
            LEFT JOIN `superb-vigil-419105.Master.hldy_mst` ON DATE (date) = hldy_date
        WHERE
            EXTRACT(
                DAYOFWEEK
                FROM
                    date
            ) NOT IN (1, 7)
            AND hldy_date IS NULL
            AND DATE (date) > DATE (date_series.np_lead_timestamp)
        GROUP BY
            np_lead_id
    ) subquery
WHERE
    main_table.np_lead_id = subquery.np_lead_id
    AND main_table.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
    AND main_table.np_st1_pln_dte IS NULL;

-- Query 2
UPDATE `superb-vigil-419105.Sales.non_partner_leads` main_table
SET
    np_st2_pln_dte = CASE
        WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
        AND np_st1_status = 'Qualified' THEN TIMESTAMP_ADD (
            TIMESTAMP_ADD (
                subquery.first_valid_date,
                INTERVAL EXTRACT(
                    HOUR
                    FROM
                        main_table.np_st1_act_dte
                ) HOUR
            ),
            INTERVAL EXTRACT(
                MINUTE
                FROM
                    main_table.np_st1_act_dte
            ) MINUTE
        )
        ELSE NULL
    END
FROM
    (
        SELECT
            np_lead_id,
            MIN(date) AS first_valid_date
        FROM
            (
                SELECT
                    np_lead_id,
                    np_st1_act_dte,
                    TIMESTAMP_ADD (
                        TIMESTAMP_TRUNC (np_st1_act_dte, DAY),
                        INTERVAL n DAY
                    ) AS date
                FROM
                    `superb-vigil-419105.Sales.non_partner_leads`
                    CROSS JOIN UNNEST (GENERATE_ARRAY (0, 30)) AS n
                WHERE
                    np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                    AND np_st1_status = 'Qualified'
                    AND np_st2_pln_dte IS NULL
            ) date_series
            LEFT JOIN `superb-vigil-419105.Master.hldy_mst` ON DATE (date) = hldy_date
        WHERE
            EXTRACT(
                DAYOFWEEK
                FROM
                    date
            ) NOT IN (1, 7)
            AND hldy_date IS NULL
            AND DATE (date) > DATE (date_series.np_st1_act_dte)
        GROUP BY
            np_lead_id
    ) subquery
WHERE
    main_table.np_lead_id = subquery.np_lead_id
    AND main_table.np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
    AND main_table.np_st1_status = 'Qualified'
    AND main_table.np_st2_pln_dte IS NULL;

-- Query 3
UPDATE `superb-vigil-419105.Sales.non_partner_leads` main_table
SET
    np_st3_pln_dte = calculated_dates.calculated_np_st3_pln_dte
FROM
    (
        WITH
            date_series AS (
                SELECT
                    np_lead_id,
                    np_lead_nbd_type,
                    np_st2_act_dte,
                    np_lead_timestamp,
                    TIMESTAMP_ADD (
                        TIMESTAMP_TRUNC (
                            CASE
                                WHEN np_lead_nbd_type NOT IN ('NBD-CRR-IN', 'NBD-CRR-OUT') THEN np_st2_act_dte
                                ELSE np_lead_timestamp
                            END,
                            DAY
                        ),
                        INTERVAL n DAY
                    ) AS date
                FROM
                    `superb-vigil-419105.Sales.non_partner_leads`
                    CROSS JOIN UNNEST (GENERATE_ARRAY (0, 60)) AS n
                WHERE
                    np_st3_pln_dte IS NULL
            ),
            working_days AS (
                SELECT
                    date_series.np_lead_id,
                    date_series.date,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            date_series.np_lead_id
                        ORDER BY
                            date_series.date
                    ) AS working_day_number
                FROM
                    date_series
                    LEFT JOIN `superb-vigil-419105.Master.hldy_mst` ON DATE (date_series.date) = hldy_date
                WHERE
                    EXTRACT(
                        DAYOFWEEK
                        FROM
                            date_series.date
                    ) NOT IN (1, 7)
                    AND hldy_date IS NULL
                    AND DATE (date_series.date) > DATE (
                        CASE
                            WHEN date_series.np_lead_nbd_type NOT IN ('NBD-CRR-IN', 'NBD-CRR-OUT') THEN date_series.np_st2_act_dte
                            ELSE date_series.np_lead_timestamp
                        END
                    )
            )
        SELECT
            main_table.np_lead_id,
            CASE
                WHEN main_table.np_lead_nbd_type NOT IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND main_table.np_st2_status != 'Lost'
                AND main_table.np_st2_act_dte IS NOT NULL THEN TIMESTAMP_ADD (
                    TIMESTAMP_ADD (
                        working_days.date,
                        INTERVAL EXTRACT(
                            HOUR
                            FROM
                                main_table.np_st2_act_dte
                        ) HOUR
                    ),
                    INTERVAL EXTRACT(
                        MINUTE
                        FROM
                            main_table.np_st2_act_dte
                    ) MINUTE
                )
                WHEN main_table.np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT') THEN TIMESTAMP_ADD (
                    TIMESTAMP_ADD (
                        working_days.date,
                        INTERVAL EXTRACT(
                            HOUR
                            FROM
                                main_table.np_lead_timestamp
                        ) HOUR
                    ),
                    INTERVAL EXTRACT(
                        MINUTE
                        FROM
                            main_table.np_lead_timestamp
                    ) MINUTE
                )
                ELSE NULL
            END AS calculated_np_st3_pln_dte
        FROM
            `superb-vigil-419105.Sales.non_partner_leads` main_table
            LEFT JOIN working_days ON main_table.np_lead_id = working_days.np_lead_id
            AND working_days.working_day_number = 5
        WHERE
            main_table.np_st3_pln_dte IS NULL
    ) calculated_dates
WHERE
    main_table.np_lead_id = calculated_dates.np_lead_id
    AND main_table.np_st3_pln_dte IS NULL
    AND calculated_dates.calculated_np_st3_pln_dte IS NOT NULL;

-- Query 4
UPDATE `superb-vigil-419105.Sales.non_partner_leads` main_table
SET
    np_st4_pln_dte = CASE
        WHEN (main_table.np_lead_nbd_type = 'CRR-NG' AND main_table.np_st3_status IS NULL) THEN 
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    subquery.first_valid_date,
                    INTERVAL EXTRACT(HOUR FROM main_table.np_lead_timestamp) HOUR
                ),
                INTERVAL EXTRACT(MINUTE FROM main_table.np_lead_timestamp) MINUTE
            )
        WHEN (main_table.np_lead_nbd_type IN ('NBD-IN','NBD-OUT','NBD-CRR-IN','NBD-CRR-OUT') AND main_table.np_st3_status ='Qualified') THEN 
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    subquery.first_valid_date,
                    INTERVAL EXTRACT(HOUR FROM main_table.np_st3_act_dte) HOUR
                ),
                INTERVAL EXTRACT(MINUTE FROM main_table.np_st3_act_dte) MINUTE
            )
        ELSE NULL
    END
FROM (
    SELECT
        np_lead_id,
        np_lead_nbd_type,
        MIN(date) AS first_valid_date
    FROM (
        SELECT
            np_lead_id,
            np_lead_nbd_type,
            np_lead_timestamp,
            np_st3_act_dte,
            TIMESTAMP_ADD(
                TIMESTAMP_TRUNC(
                    CASE
                        WHEN np_lead_nbd_type = 'CRR-NG' THEN np_lead_timestamp
                        ELSE np_st3_act_dte
                    END,
                    DAY
                ),
                INTERVAL n DAY
            ) AS date,
            np_st3_status
        FROM
            `superb-vigil-419105.Sales.non_partner_leads`
            CROSS JOIN UNNEST(GENERATE_ARRAY(0, 30)) AS n
        WHERE np_st4_pln_dte IS NULL
    ) date_series
    LEFT JOIN `superb-vigil-419105.Master.hldy_mst` ON DATE(date) = hldy_date
    WHERE
        EXTRACT(DAYOFWEEK FROM date) NOT IN (1, 7)
        AND hldy_date IS NULL
        AND DATE(date) > DATE(
            CASE
                WHEN date_series.np_lead_nbd_type = 'CRR-NG' THEN date_series.np_lead_timestamp
                ELSE date_series.np_st3_act_dte
            END
        )
    GROUP BY
        np_lead_id,
        np_lead_nbd_type
) subquery
WHERE
    main_table.np_lead_id = subquery.np_lead_id
    AND main_table.np_st4_pln_dte IS NULL;
UPDATE `superb-vigil-419105.Sales.non_partner_leads`
SET
    np_final_status = CASE
        WHEN np_lead_id IS NOT NULL THEN CASE -- Case 1
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_pln_dte IS NOT NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(np_st1_status, 'No Status Updated') -- Case 2
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st2_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NOT NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(np_st2_status, np_st1_status, 'Error') -- Case 3
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st3_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(
                np_st3_status,
                np_st2_status,
                np_st1_status,
                'No Status Updated'
            ) ----- Case 4
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st4_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st3_pln_dte IS NOT NULL THEN COALESCE(np_st4_status, np_st3_status, 'error') -- Case 5
            WHEN np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
            AND np_st3_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN COALESCE(np_st3_status, 'No Status Updated') ---------Case 6
            WHEN np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
            AND np_st4_pln_dte IS NOT NULL
            AND np_st3_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NULL
            AND np_st2_pln_dte IS NULL THEN COALESCE(np_st4_status, np_st3_status, 'No Status Updated') -- Case 7
            WHEN np_lead_nbd_type = 'CRR-NG'
            AND np_st4_pln_dte IS NOT NULL
            AND np_st1_pln_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL THEN COALESCE(np_st4_status, 'Completed')
            ELSE np_final_status -- Keep the existing value if none of the conditions are met
        END
        ELSE np_final_status -- Keep the existing value if np_lead_id is NULL
    END
WHERE
    np_lead_id IS NOT NULL;

END;


UPDATE job_tracker
SET
    end_time = CURRENT_TIMESTAMP()
WHERE
    job_id = job_id_2;

-- Wait for Query 2 to complete
CALL `superb-vigil-419105.Sales.wait_for_job` (job_id_2);

-- Query 3: Merge into np_db_format_lead_capture
SET
    job_id_3 = GENERATE_UUID ();

INSERT INTO
    job_tracker (job_id, step, start_time)
VALUES
    (job_id_3, 'Query3', CURRENT_TIMESTAMP());

TRUNCATE TABLE `superb-vigil-419105.Sales.np_db_format_lead_capture`;
MERGE `superb-vigil-419105.Sales.np_db_format_lead_capture` T USING (
    SELECT
        np_lead_id,
        np_lead_company_name,
        np_lead_name,
        np_lead_mail,
        np_lead_contact_no,
        np_lead_descrptn,
        np_lead_country,
        np_lead_sc_assigned,
        np_lead_salesperson,
        CASE
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_pln_dte IS NOT NULL
            AND np_st1_act_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN 'LC1'
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_act_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st2_act_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN 'LC2'
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st4_pln_dte IS NULL
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_act_dte IS NULL
            ) THEN 'LC3'
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NOT NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type = 'CRR-NG'
                AND np_st4_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_pln_dte IS NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_act_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            ) THEN 'LC4'
            WHEN np_st4_act_dte IS NOT NULL THEN 'Completed'
            ELSE 'Unknown'
        END AS current_step,
        CASE
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_pln_dte IS NOT NULL
            AND np_st1_act_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN np_st1_pln_dte
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_act_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st2_act_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN np_st2_pln_dte
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st4_pln_dte IS NULL
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_act_dte IS NULL
            ) THEN np_st3_pln_dte
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NOT NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type = 'CRR-NG'
                AND np_st4_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_pln_dte IS NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_act_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            ) THEN np_st4_pln_dte
            WHEN np_st4_act_dte IS NOT NULL THEN CAST('Completed' AS TIMESTAMP)
        END AS current_pln_dte,
        CASE
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_pln_dte IS NOT NULL
            AND np_st1_act_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN 'L1 ACCOUNT QUALIFICATION'
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_act_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st2_act_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN 'INTRO MEETING'
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st4_pln_dte IS NULL
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_act_dte IS NULL
            ) THEN 'FK & FEASIBILITY'
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NOT NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type = 'CRR-NG'
                AND np_st4_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_pln_dte IS NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_act_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            ) THEN 'COMPLETE DATA ENTRY'
            WHEN np_st4_act_dte IS NOT NULL THEN 'Completed'
            ELSE 'Unknown'
        END AS step_name,
        CASE
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_pln_dte IS NOT NULL
            AND np_st1_act_dte IS NULL
            AND np_st2_pln_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN 'CHECK ACCOUNT DETAILS'
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_act_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st2_act_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN 'GET INTRO MEETING'
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st4_pln_dte IS NULL
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_act_dte IS NULL
            ) THEN 'CHECK PRODUCT FEASIBILITY'
            WHEN (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_act_dte IS NOT NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type = 'CRR-NG'
                AND np_st4_pln_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_pln_dte IS NULL
                AND np_st4_act_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_act_dte IS NOT NULL
                AND np_st1_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
            ) THEN 'PUSH DATA IN FUNNEL'
            WHEN np_st4_act_dte IS NOT NULL THEN 'Completed'
            ELSE 'Unknown'
        END AS how,
        np_lead_nbd_type,
        np_final_status,
        np_lead_source,
        np_lead_region,
        np_lead_product,
 CASE
    WHEN (np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT') AND np_st3_status IS NOT NULL AND np_st4_pln_dte IS NOT NULL AND np_st4_act_dte IS NULL) THEN 
        CONCAT(
            "https://docs.google.com/forms/d/e/1FAIpQLScD2ihbOnF4Dd18XrnMN3zagvu2gnb6e0776TaHgfyQWYhQuA/viewform?usp=pp_url",
            "&entry.893331085=", IFNULL(np_lead_id, ""),
            "&entry.2054893244=", IFNULL(np_lead_company_name, ""),
            "&entry.60587760=", IFNULL(np_lead_name, ""),
            "&entry.1905038195=", IFNULL(np_lead_product, ""),
            "&entry.905256428=", IFNULL(np_lead_descrptn, ""),
            "&entry.480796560=", IFNULL(np_lead_contact_no, ""),
            "&entry.936229720=", IFNULL(np_lead_mail, ""),
            "&entry.1510541529=", IFNULL(np_lead_region, ""),
            "&entry.824129851=", IFNULL(np_lead_nbd_type, ""),
            "&entry.1259203622=", IFNULL(np_lead_source, ""),
            "&entry.200797182=", IFNULL(np_lead_sc_assigned, ""),
            "&entry.457302474=", IFNULL(np_lead_salesperson, "")
        )
    WHEN (np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT') AND np_st3_status IS NOT NULL AND np_st4_pln_dte IS NOT NULL AND np_st4_act_dte IS NULL) THEN 
        CONCAT(
            "https://docs.google.com/forms/d/e/1FAIpQLSekfA2EJQLS9Na4mDV2FOrw94rxNerPaaxd69-le8kgE3DjZA/viewform?usp=pp_url",
            "&entry.1438189531=", IFNULL(np_lead_id, ""),
            "&entry.1686607363=", IFNULL(np_lead_company_name, ""),
            "&entry.725612070=", IFNULL(np_lead_name, ""),
            "&entry.2058409493=", IFNULL(np_lead_product, ""),
            "&entry.373601117=", IFNULL(np_lead_descrptn, ""),
            "&entry.1448658513=", IFNULL(np_lead_contact_no, ""),
            "&entry.1747921117=", IFNULL(np_lead_mail, ""),
            "&entry.1051061487=", IFNULL(np_lead_region, ""),
            "&entry.1845704016=", IFNULL(np_lead_nbd_type, ""),
            "&entry.807291501=", IFNULL(np_lead_source, ""),
            "&entry.1162899163=", IFNULL(np_lead_sc_assigned, ""),
            "&entry.136023687=", IFNULL(np_lead_salesperson, ""),
            "&entry.345438663=", IFNULL(np_lead_industry, ""),
            "&entry.1627958016=", IFNULL(np_lead_customer_type, "")
        )
    WHEN (np_lead_nbd_type = 'CRR-NG' AND np_st4_pln_dte IS NOT NULL AND np_st4_act_dte IS NULL) THEN 
        CONCAT(
            "https://docs.google.com/forms/d/e/1FAIpQLSfV6GoKdra7C44MwboDhXt8dWT_5zTZ4LCq4_xLsPXBvS32qw/viewform?usp=pp_url",
            "&entry.1438189531=", IFNULL(np_lead_id, ""),
            "&entry.1686607363=", IFNULL(np_lead_company_name, ""),
            "&entry.725612070=", IFNULL(np_lead_name, ""),
            "&entry.2058409493=", IFNULL(np_lead_product, ""),
            "&entry.373601117=", IFNULL(np_lead_descrptn, ""),
            "&entry.1448658513=", IFNULL(np_lead_contact_no, ""),
            "&entry.1747921117=", IFNULL(np_lead_mail, ""),
            "&entry.1051061487=", IFNULL(np_lead_region, ""),
            "&entry.1845704016=NBD-CRR-IN",
            "&entry.807291501=", IFNULL(np_lead_source, ""),
            "&entry.1162899163=", IFNULL(np_lead_sc_assigned, ""),
            "&entry.136023687=", IFNULL(np_lead_salesperson, "")
        )
    ELSE ''
END AS to_funnel_link
,
        CASE
            WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
            AND np_st1_act_dte IS NOT NULL
            AND np_st2_pln_dte IS NOT NULL
            AND np_st2_act_dte IS NULL
            AND np_st3_pln_dte IS NULL
            AND np_st4_pln_dte IS NULL THEN CONCAT (
                "https://docs.google.com/forms/d/e/1FAIpQLSc4qUh72EUKYnPD5bKX0Ro6hr6QNIFoY6wyaTvDxAY-5N209g/viewform?usp=pp_url",
                "&entry.61557590=",
                IFNULL (np_lead_id, ""),
                "&entry.968923407=",
                'LC2'
            )
            ELSE CONCAT (
                "https://docs.google.com/forms/d/e/1FAIpQLScYgCRvGbFvLASa1fIGQFyCKQ0IYbVNUpwl5stylHuZy6rifA/viewform?usp=pp_url",
                "&entry.1793350281=",
                IFNULL (np_lead_id, ""),
                "&entry.583154962=",
                IFNULL (
                    CASE
                        WHEN np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                        AND np_st1_pln_dte IS NOT NULL
                        AND np_st1_act_dte IS NULL
                        AND np_st2_pln_dte IS NULL
                        AND np_st3_pln_dte IS NULL
                        AND np_st4_pln_dte IS NULL THEN 'LC1'
                        WHEN (
                            np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                            AND np_st3_pln_dte IS NOT NULL
                            AND np_st4_pln_dte IS NULL
                            AND np_st1_act_dte IS NOT NULL
                            AND np_st2_act_dte IS NOT NULL
                            AND np_st3_act_dte IS NULL
                        )
                        OR (
                            np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                            AND np_st3_pln_dte IS NOT NULL
                            AND np_st1_pln_dte IS NULL
                            AND np_st2_pln_dte IS NULL
                            AND np_st3_act_dte IS NULL
                        ) THEN 'LC3'
                        WHEN (
                            np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                            AND np_st1_act_dte IS NOT NULL
                            AND np_st2_act_dte IS NOT NULL
                            AND np_st3_act_dte IS NOT NULL
                            AND np_st4_pln_dte IS NOT NULL
                            AND np_st4_act_dte IS NULL
                        )
                        OR (
                            np_lead_nbd_type = 'CRR-NG'
                            AND np_st4_pln_dte IS NOT NULL
                            AND np_st1_pln_dte IS NULL
                            AND np_st2_pln_dte IS NULL
                            AND np_st3_pln_dte IS NULL
                            AND np_st4_act_dte IS NULL
                        )
                        OR (
                            np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                            AND np_st3_act_dte IS NOT NULL
                            AND np_st1_pln_dte IS NULL
                            AND np_st2_pln_dte IS NULL
                            AND np_st4_pln_dte IS NOT NULL
                            AND np_st4_act_dte IS NULL
                        ) THEN 'LC4'
                        WHEN np_st4_act_dte IS NOT NULL THEN 'Completed'
                        ELSE 'Unknown'
                    END,
                    ""
                )
            )
        END AS link_to_complete
    FROM
        `superb-vigil-419105.Sales.non_partner_leads`
    WHERE
        (
            (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st1_pln_dte IS NOT NULL
                AND np_st1_act_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st3_pln_dte IS NULL
                AND np_st4_pln_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st1_act_dte IS NOT NULL
                AND np_st2_pln_dte IS NOT NULL
                AND np_st2_act_dte IS NULL
                AND np_st3_pln_dte IS NULL
                AND np_st4_pln_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st2_act_dte IS NOT NULL
                AND np_st3_pln_dte IS NOT NULL
                AND np_st3_act_dte IS NULL
                AND np_st1_pln_dte IS NOT NULL
                AND np_st2_pln_dte IS NOT NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-IN', 'NBD-OUT')
                AND np_st4_act_dte IS NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st3_act_dte IS NOT NULL
                AND np_st1_pln_dte IS NOT NULL
                AND np_st2_pln_dte IS NOT NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT')
                AND np_st3_pln_dte IS NOT NULL
                AND np_st3_act_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st1_pln_dte IS NULL
            )
            OR (
                np_lead_nbd_type IN ('NBD-CRR-IN', 'NBD-CRR-OUT', 'NBD-IN', 'NBD-OUT')
                AND np_st3_act_dte IS NOT NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st4_act_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st1_pln_dte IS NULL
            )
            OR (
                np_lead_nbd_type = 'CRR-NG'
                AND np_st4_act_dte IS NULL
                AND np_st4_pln_dte IS NOT NULL
                AND np_st3_pln_dte IS NULL
                AND np_st2_pln_dte IS NULL
                AND np_st1_pln_dte IS NULL
            )
        )
        AND np_lead_id IS NOT NULL
) S ON T.np_lead_id = S.np_lead_id WHEN MATCHED THEN
UPDATE
SET
    np_lead_company_name = S.np_lead_company_name,
    np_lead_name = S.np_lead_name,
    np_lead_mail = S.np_lead_mail,
    np_lead_contact_no = S.np_lead_contact_no,
    np_lead_descrptn = S.np_lead_descrptn,
    np_lead_country = S.np_lead_country,
    np_lead_sc_assigned = S.np_lead_sc_assigned,
    np_lead_salesperson = S.np_lead_salesperson,
    current_step = S.current_step,
    current_pln_dte = S.current_pln_dte,
    step_name = S.step_name,
    how = S.how,
    np_lead_nbd_type = S.np_lead_nbd_type,
    np_final_status = S.np_final_status,
    np_lead_source = S.np_lead_source,
    np_lead_region = S.np_lead_region,
    np_lead_product = S.np_lead_product,
    to_funnel_link = S.to_funnel_link,
    link_to_complete = S.link_to_complete WHEN NOT MATCHED THEN INSERT (
        np_lead_id,
        np_lead_company_name,
        np_lead_name,
        np_lead_mail,
        np_lead_contact_no,
        np_lead_descrptn,
        np_lead_country,
        np_lead_sc_assigned,
        np_lead_salesperson,
        current_step,
        current_pln_dte,
        step_name,
        how,
        np_lead_nbd_type,
        np_final_status,
        np_lead_source,
        np_lead_region,
        np_lead_product,
        to_funnel_link,
        link_to_complete
    )
VALUES
    (
        S.np_lead_id,
        S.np_lead_company_name,
        S.np_lead_name,
        S.np_lead_mail,
        S.np_lead_contact_no,
        S.np_lead_descrptn,
        S.np_lead_country,
        S.np_lead_sc_assigned,
        S.np_lead_salesperson,
        S.current_step,
        S.current_pln_dte,
        S.step_name,
        S.how,
        S.np_lead_nbd_type,
        S.np_final_status,
        S.np_lead_source,
        S.np_lead_region,
        S.np_lead_product,
        S.to_funnel_link,
        S.link_to_complete
    );

UPDATE job_tracker
SET
    end_time = CURRENT_TIMESTAMP()
WHERE
    job_id = job_id_3;

-- Wait for Query 3 to complete
CALL `superb-vigil-419105.Sales.wait_for_job` (job_id_3);

-- Log completion or perform any final operations
SELECT
    'All queries completed successfully' AS status;

-- Optionally, you can log the execution times
SELECT
    step,
    TIMESTAMP_DIFF (end_time, start_time, SECOND) AS execution_time_seconds
FROM
    job_tracker
ORDER BY
    start_time;

END;
