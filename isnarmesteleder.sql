
/*kobletnarmestelederaktivt*/
SELECT oversikt.opprettet,oversikt.id AS oversikt_id,oversikt.uuid AS oversikt_uuid,oversikt.fnr ,leder.created_at as leder_created_at,leder.virksomhetsnummer,leder.narmeste_leder_personident,leder.arbeidstaker_personident,leder.aktiv_fom,leder.aktiv_tom,leder.status FROM EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.syfooversikt", "SELECT id,opprettet, uuid, fnr FROM person_oversikt_status WHERE(motebehov_ubehandlet = 't' OR oppfolgingsplan_lps_bistand_ubehandlet = 't' OR dialogmotesvar_ubehandlet = 't' OR (dialogmotekandidat = 't' AND dialogmotekandidat_generated_at + INTERVAL '7 DAY' < now()) OR behandlerdialog_svar_ubehandlet = 't' OR behandlerdialog_ubesvart_ubehandlet = 't' OR behandlerdialog_avvist_ubehandlet = 't' OR trenger_oppfolging = 't'  OR behandler_bistand_ubehandlet = 't' OR arbeidsuforhet_aktiv_vurdering = 't' OR friskmelding_til_arbeidsformidling_fom IS NOT NULL OR is_aktiv_sen_oppfolging_kandidat = 't' OR is_aktiv_aktivitetskrav_vurdering = 't' OR is_aktiv_manglende_medvirkning_vurdering = 't')") AS oversikt LEFT JOIN  EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.narmesteleder", "SELECT created_at, arbeidstaker_personident,virksomhetsnummer, narmeste_leder_personident, status, aktiv_tom,aktiv_fom FROM narmeste_leder_relasjon") AS leder ON oversikt.fnr = leder.arbeidstaker_personident;




/*narmesteleder_restricted*/
SELECT * FROM EXTERNAL_QUERY("teamsykefravr-prod-7e29.europe-north1.narmesteleder", "SELECT created_at, updated_at, virksomhetsnummer, arbeidstaker_personident, narmeste_leder_personident, aktiv_fom, aktiv_tom, status FROM narmeste_leder_relasjon where created_at >= '2025-01-01';");


