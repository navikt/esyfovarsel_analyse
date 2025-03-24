/*esyfovarsel_alt*/
SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT type, utsendt_tidspunkt, kanal FROM utsendt_varsel;");


