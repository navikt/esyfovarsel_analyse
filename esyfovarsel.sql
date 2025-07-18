/*esyfovarsel_alt*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT type, utsendt_tidspunkt, kanal FROM utsendt_varsel;");

/*esyfovarsel_feilet_utsending*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT cast(uuid as text), cast(uuid_ekstern_referanse as text), arbeidstaker_fnr, hendelsetype_navn, orgnummer,arbeidsgivernotifikasjon_merkelapp, brukernotifikasjoner_melding_type,kanal, feilmelding, utsendt_forsok_tidspunkt, is_forced_letter, is_resendt, resendt_tidspunkt FROM utsending_varsel_feilet;");

/*esyfovarsel_feilet_utsending_yesterday*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > '2025-04-01' and  is_resendt=false;");

/*esyfovarsel_kalenderavtale*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT cast(id as text), eksternid, sakid, grupperingsid, merkelapp, kalenderid, starttidspunkt, slutttidspunkt, kalenderavtaletilstand, harddeletedate, opprettet FROM arbeidsgivernotifikasjoner_kalenderavtale;");

/*esyfovarsel_mikrofrontend_synlighet*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT synlig_for, tjeneste, synlig_tom, opprettet, sist_endret FROM mikrofrontend_synlighet;");

/*isyfo_mote_status_endret*/
SELECT * FROM EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.dialogmote", "SELECT cast(id as text), cast(uuid as text),created_at,updated_at,cast(mote_id as text), status, opprettet_av, tilfelle_start, published_at,motedeltaker_behandler  FROM mote_status_endret;");

/*isyfo_friskmelding_til_arbeidsformidling*/
SELECT * FROM EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.frisktilarbeid", "SELECT created_at FROM vedtak;");

/*isyfo_dialog_status_endret_sykmeldte*/
SELECT ms.status,ms.motedeltaker_behandler, ms.mote_id, ma.personident, ms.created_at as created_at_mse, ma.created_at as created_as_ma FROM EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.dialogmote", "SELECT * FROM MOTE_STATUS_ENDRET WHERE status = 'NYTT_TID_STED'") ms INNER JOIN EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.dialogmote", "SELECT * FROM motedeltaker_arbeidstaker") ma ON ms.mote_id = ma.mote_id;



