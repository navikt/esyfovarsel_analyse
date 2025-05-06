/*esyfovarsel_alt*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT type, utsendt_tidspunkt, kanal FROM utsendt_varsel;");

/*esyfovarsel_feilet_utsending*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT cast(uuid as text), cast(uuid_ekstern_referanse as text), arbeidstaker_fnr, hendelsetype_navn, orgnummer,arbeidsgivernotifikasjon_merkelapp, brukernotifikasjoner_melding_type,kanal, feilmelding, utsendt_forsok_tidspunkt, is_forced_letter, is_resendt, resendt_tidspunkt FROM utsending_varsel_feilet;");

/*esyfovarsel_kalenderavtale*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT cast(id as text), eksternid, sakid, grupperingsid, merkelapp, kalenderid, starttidspunkt, slutttidspunkt, kalenderavtaletilstand, harddeletedate, opprettet FROM arbeidsgivernotifikasjoner_kalenderavtale;");

/*esyfovarsel_mikrofrontend_synlighet*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT synlig_for, tjeneste, synlig_tom, opprettet, sist_endret FROM mikrofrontend_synlighet;");

/*isyfo_mote_status_endret*/
SELECT * FROM EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.dialogmote", "SELECT cast(id as text), cast(uuid as text),created_at,updated_at,cast(mote_id as text), status, opprettet_av, tilfelle_start, published_at,motedeltaker_behandler  FROM mote_status_endret;");
