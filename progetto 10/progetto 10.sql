/* 
Lo scopo è creare le feature per un possibile modello di machine learning supervisionato
Ogni indicatore va riferito al singolo id_cliente.
*/

-- Età
CREATE TEMPORARY TABLE età AS
SELECT id_cliente, 
       TIMESTAMPDIFF(YEAR, data_nascita, CURDATE()) AS età
FROM cliente;


-- Numero di transazioni in uscita su tutti i conti
CREATE TEMPORARY TABLE num_transazioni_uscita AS
SELECT c.id_cliente, 
       COUNT(t.id_tipo_trans) AS num_transazioni_uscita
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON t.id_conto = co.id_conto AND t.id_tipo_trans >= 3
GROUP BY c.id_cliente;


-- Numero di transazioni in entrata su tutti i conti
CREATE TEMPORARY TABLE num_transazioni_entrata AS
SELECT c.id_cliente, 
       COUNT(t.id_tipo_trans) AS num_transazioni_entrata
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON t.id_conto = co.id_conto AND t.id_tipo_trans < 3
GROUP BY c.id_cliente;


-- Importo transato in uscita su tutti i conti
CREATE TEMPORARY TABLE importo_uscita AS
SELECT c.id_cliente, 
      IFNULL(SUM(t.importo), 0) AS importo_uscita
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON co.id_conto = t.id_conto AND t.importo < 0
GROUP BY c.id_cliente;

ALTER TABLE importo_uscita 
MODIFY COLUMN importo_uscita DECIMAL(15,2);


-- Importo transato in entrata su tutti i conti
CREATE TEMPORARY TABLE importo_entrata AS
SELECT c.id_cliente, 
       IFNULL(SUM(t.importo), 0) AS importo_entrata
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON co.id_conto = t.id_conto AND t.importo > 0
GROUP BY c.id_cliente;

ALTER TABLE importo_entrata
MODIFY COLUMN importo_entrata DECIMAL(15,2);


-- Numero totale di conti posseduti
CREATE TEMPORARY TABLE conti_posseduti AS
SELECT c.id_cliente,
       COUNT(co.id_conto) AS conti_posseduti
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
GROUP BY c.id_cliente;


-- Numero di conti posseduti per tipologia (un indicatore per tipo)
CREATE TEMPORARY TABLE conti_per_tipo AS
SELECT c.id_cliente,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 0 THEN 1 ELSE 0 END), 0) AS conto_base,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 1 THEN 1 ELSE 0 END), 0) AS conto_business,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 2 THEN 1 ELSE 0 END), 0) AS conto_privati,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 3 THEN 1 ELSE 0 END), 0) AS conto_famiglia
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
GROUP BY c.id_cliente;


-- Numero di transazioni in uscita per tipologia (un indicatore per tipo)
CREATE TEMPORARY TABLE transazioni_uscita AS
SELECT c.id_cliente,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 3 THEN 1 ELSE 0 END), 0) AS num_trans_uscita_3,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 4 THEN 1 ELSE 0 END), 0) AS num_trans_uscita_4,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 5 THEN 1 ELSE 0 END), 0) AS num_trans_uscita_5,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 6 THEN 1 ELSE 0 END), 0) AS num_trans_uscita_6,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 7 THEN 1 ELSE 0 END), 0) AS num_trans_uscita_7
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON co.id_conto = t.id_conto
GROUP BY c.id_cliente;


-- Numero di transazioni in entrata per tipologia (un indicatore per tipo)
CREATE TEMPORARY TABLE transazioni_entrata AS
SELECT c.id_cliente,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 0 THEN 1 ELSE 0 END), 0) AS num_trans_entrata_0,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 1 THEN 1 ELSE 0 END), 0) AS num_trans_entrata_1,
       IFNULL(SUM(CASE WHEN t.id_tipo_trans = 2 THEN 1 ELSE 0 END), 0) AS num_trans_entrata_2
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON co.id_conto = t.id_conto
GROUP BY c.id_cliente;


-- Importo transato in uscita per tipologia di conto (un indicatore per tipo)
CREATE TEMPORARY TABLE importo_uscita_conto AS
SELECT c.id_cliente,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 0 AND t.importo < 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_uscita_conto_0,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 1 AND t.importo < 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_uscita_conto_1,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 2 AND t.importo < 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_uscita_conto_2,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 3 AND t.importo < 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_uscita_conto_3
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON co.id_conto = t.id_conto
GROUP BY c.id_cliente;
    

-- Importo transato in entrata per tipologia di conto (un indicatore per tipo)
CREATE TEMPORARY TABLE importo_entrata_conto AS
SELECT c.id_cliente,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 0 AND t.importo > 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_entrata_conto_0,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 1 AND t.importo > 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_entrata_conto_1,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 2 AND t.importo > 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_entrata_conto_2,
       IFNULL(SUM(CASE WHEN co.id_tipo_conto = 3 AND t.importo > 0 THEN t.importo ELSE 0 END), 0) AS importo_trans_entrata_conto_3
FROM cliente c
LEFT JOIN conto co ON c.id_cliente = co.id_cliente
LEFT JOIN transazioni t ON co.id_conto = t.id_conto
GROUP BY c.id_cliente;




-- JOIN DI TUTTE LE TABELLE TEMPORANEE CON L'ID_CLIENTE
SELECT c.id_cliente,
       età.età,
       ntu.num_transazioni_uscita,
       nte.num_transazioni_entrata,
       iou.importo_uscita,
       ine.importo_entrata,
       cp.conti_posseduti,
       cpt.conto_base,
       cpt.conto_business,
       cpt.conto_privati,
       cpt.conto_famiglia,
       tu.num_trans_uscita_3,
       tu.num_trans_uscita_4,
       tu.num_trans_uscita_5,
       tu.num_trans_uscita_6,
       tu.num_trans_uscita_7,
       te.num_trans_entrata_0,
       te.num_trans_entrata_1,
       te.num_trans_entrata_2,
       iouc.importo_trans_uscita_conto_0,
       iouc.importo_trans_uscita_conto_1,
       iouc.importo_trans_uscita_conto_2,
       iouc.importo_trans_uscita_conto_3,
       ienc.importo_trans_entrata_conto_0,
       ienc.importo_trans_entrata_conto_1,
       ienc.importo_trans_entrata_conto_2,
       ienc.importo_trans_entrata_conto_3
FROM cliente c
LEFT JOIN età età ON c.id_cliente = età.id_cliente
LEFT JOIN num_transazioni_uscita ntu ON c.id_cliente = ntu.id_cliente
LEFT JOIN num_transazioni_entrata nte ON c.id_cliente = nte.id_cliente
LEFT JOIN importo_uscita iou ON c.id_cliente = iou.id_cliente
LEFT JOIN importo_entrata ine ON c.id_cliente = ine.id_cliente
LEFT JOIN conti_posseduti cp ON c.id_cliente = cp.id_cliente
LEFT JOIN conti_per_tipo cpt ON c.id_cliente = cpt.id_cliente
LEFT JOIN transazioni_uscita tu ON c.id_cliente = tu.id_cliente
LEFT JOIN transazioni_entrata te ON c.id_cliente = te.id_cliente
LEFT JOIN importo_uscita_conto iouc ON c.id_cliente = iouc.id_cliente
LEFT JOIN importo_entrata_conto ienc ON c.id_cliente = ienc.id_cliente;






