SELECT (i.raw_json ->> 'id')::integer                                                AS id
     , CURRENT_DATE - TO_DATE((i.raw_json -> 'fields' ->> 'duedate'), 'DD/MM/YYYY')  AS latedays
     , i.raw_json -> 'fields' ->> 'tranid'                                           AS tranid                --id IN excel
     , TO_DATE((i.raw_json -> 'fields' ->> 'prevdate'), 'DD/MM/YYYY')                AS "date"                -- date IN excel
     , (i.raw_json -> 'fields' ->> 'total')::numeric                                 AS total_amount
     , CASE
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'H' THEN 'Kpler INC'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'L' THEN 'Kpler LTD'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'P' THEN 'Kpler SAS'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'S' THEN 'Kpler PTE LTD'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'D' THEN 'Kpler DMCC'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'E' THEN 'Kpler Exmile'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'R' THEN 'Kpler Germany'
           WHEN RIGHT((i.raw_json -> 'fields' ->> 'tranid'), 1) = 'V' THEN 'Kpler GMBH'
           ELSE 'Kpler Other'
    END                                                                              AS entity
     , so.branch__c                                                                  AS branch
     , i.raw_json -> 'fields' ->> 'custbody_kpl_contract_num'                        AS contract_num
     , vcga.group_account_name || ' : ' || vcga.account_name                         AS client
     , i.raw_json -> 'fields' ->> 'currencyname'                                     AS currencyname
     , i.raw_json -> 'fields' ->> 'custbody_kpl_installment_number'                  AS installment
     , ROUND(1 / COALESCE(cu.currency_rate, 1), 2)                                   AS fx
     , (i.raw_json -> 'fields' ->> 'subtotal')::numeric                              AS amount_before_tax
     , (i.raw_json -> 'fields' ->> 'taxtotal')::numeric                              AS tax
     , TO_DATE((i.raw_json -> 'fields' ->> 'duedate'), 'DD/MM/YYYY')                 AS due_date
     --SOURCE ?? it comes from the invoice and salesforce referance_po purchase order refernace/ clietns purchase order
     , ROUND((i.raw_json -> 'fields' ->> 'subtotal')::numeric /
             COALESCE(cu.currency_rate, 1), 2)                                       AS euro_amount_before_tax
     , ROUND((i.raw_json -> 'fields' ->> 'taxtotal')::numeric /
             COALESCE(cu.currency_rate, 1), 2)                                       AS euro_tax
     , ROUND((i.raw_json -> 'fields' ->> 'total')::numeric /
             COALESCE(cu.currency_rate, 1), 2)                                       AS euro_total_amount
     , i.raw_json -> 'fields' ->> 'status'                                           AS status
     , CASE
           WHEN TO_DATE((p.raw_json -> 'fields' ->> 'trandate'), 'DD/MM/YYYY') IS NOT NULL
               THEN TO_DATE((i.raw_json -> 'fields' ->> 'duedate'), 'DD/MM/YYYY') --if invoice is paid then paid_date=payment dat
    END                                                                              AS paid_date
     --if invoice status is paid then paid_date=payment date
     --if status is open or current  (pending) then today
     --if status is late then due_date+1 month
     --alerts? skip it
     --last_alert  skip i
     , su."name"                                                                     AS sales_rep
     --comment? skipp
     , CURRENT_DATE - TO_DATE((i.raw_json -> 'fields' ->> 'prevdate'), 'DD/MM/YYYY') AS age
     , TO_DATE((p.raw_json -> 'fields' ->> 'trandate'), 'DD/MM/YYYY')                AS "actual_payment_date" -- date IN excel
     , i.raw_json -> 'fields' ->> 'custbody_kpl_installment_id'                         ns_inv_id
     , p.id                                                                          AS payment_id
     , (p.raw_json -> 'fields' ->> 'payment')::numeric                               AS amount_paid
     , ROUND((p.raw_json -> 'fields' ->> 'payment')::numeric /
             COALESCE(cu.currency_rate, 1), 2)                                       AS euro_amount_paid
     , (c.raw_json ->> 'id')::integer                                                AS credit_id
     , c.raw_json -> 'fields'->>'custbody_psg_sal_sg_createdfr' as invoice_referanced
FROM lake_netsuite.kpler_netsuite_invoices i
         JOIN lake_salesforce.sf_opportunities so
              ON so.id = JSONB_EXTRACT_PATH_TEXT(i.raw_json, 'fields', 'custbody_kpl_opportunity_id')
         JOIN lake_salesforce.sf_users su
              ON so.ownerid = su.id
         JOIN dw.v_crm_group_accounts vcga
              ON so.accountid = vcga.sf_account_id
         LEFT JOIN dw.currencies_eur cu
                   ON i.raw_json -> 'fields' ->> 'currencyname' = cu.currency_code
                       AND cu.currency_date = CURRENT_DATE
         LEFT JOIN lake_netsuite.kpler_netsuite_payments p
                   ON JSONB_EXTRACT_PATH_TEXT(p.raw_json, 'sublists', 'apply', 'line 1', 'doc') =
                      JSONB_EXTRACT_PATH_TEXT(i.raw_json, 'fields', 'id')
         LEFT JOIN lake_netsuite.kpler_netsuite_credits c
                   ON JSONB_EXTRACT_PATH_TEXT(c.raw_json, 'fields', 'createdfrom') =
                      JSONB_EXTRACT_PATH_TEXT(i.raw_json, 'fields', 'id')
WHERE i.raw_json -> 'fields' ->> 'status' in ('Paid In Full','Open')
AND   i.raw_json -> 'fields' ->> 'custbody_kpl_contract_num' = '44052918';
--i.raw_json -> 'fields' ->> 'status'           <> 'Paid In Full'


--'fields' ->> 'custbody_kpl_contract_num' = '44055849'  payment from dispute
--'fields' ->> 'custbody_kpl_contract_num' = '44052918'  example to understand

-- i need to invoice referenace id. to understand credit I need the invoice ref to start with Invoice # -->> this was credited , not paid by the customer


SELECT *
FROM lake_netsuite.kpler_netsuite_invoices i
--WHERE i.raw_json -> 'fields' ->> 'tranid' = '20240523-0038S'

--i.raw_json -> 'fields' ->> 'custbody_kpl_contract_num' = '44052918'


SELECT *
FROM lake_netsuite.kpler_netsuite_payments p
WHERE (p.raw_json ->> 'id')::integer = 1766801
--JSONB_EXTRACT_PATH_TEXT(p.raw_json, 'sublists', 'apply', 'line 1', 'doc') = '1766801'


SELECT DISTINCT i.raw_json -> 'fields' ->> 'status'
FROM lake_netsuite.kpler_netsuite_invoices i
--WHERE i.raw_json -> 'fields' ->> 'custbody_kpl_contract_num' = '44051411'


select * from dw.v_netsuite_payments_to_invoices
where payment_id=1544658


select * from lake_netsuite.kpler_netsuite_invoices i --1497382
where --(i.raw_json ->> 'id')::integer=1967485
i.raw_json -> 'fields' ->> 'tranid' ='20240422-0017C'